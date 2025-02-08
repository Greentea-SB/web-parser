import asyncio
import base64
import gspread
import logging
import os
import random
import time
from datetime import datetime
from playwright.async_api import async_playwright
from oauth2client.service_account import ServiceAccountCredentials

# Конфигурация
CONFIG = {
    "SPREADSHEET_ID": "1loVjBMvaO-Ia5JnzMTz8YaGqq10XDz-L1LRWNDDVzsE",
    "SHEET_NAME": "pars",
    "CREDS_FILE": "temp_key.json",
    "MAX_RETRIES": 3,
    "MAX_NA_RETRIES": 5,
    "REQUEST_DELAY": 5,
    "MAX_CONCURRENT_PAGES": 10,
    "START_ROW": 14,
    "TOTAL_URLS": 260,
    "ERROR_VALUES": {"0", "--%", "0%", "N/A", "FAIL", "", " "},
    "TARGET_CLASSES": {
        'col_d': ['css-16udrhy', 'css-16udrhy', 'css-nd24it'],
        'col_e': ['css-sahmrr', 'css-kavdos', 'css-1598eja'],
        'col_f': ['css-j4xe5q', 'css-d865bw', 'css-krr03m']
    },
    "API_DELAY": 1.2
}

def setup_logging():
    """Инициализация системы логирования"""
    logging.basicConfig(
        level=logging.DEBUG,  # Изменено на DEBUG для подробного лога
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler("parser.log"),
            logging.StreamHandler()
        ]
    )

def clean_numeric_values(data_list):
    """Очистка числовых значений с валидацией"""
    cleaned = []
    for item in data_list:
        try:
            # Удаление нежелательных символов
            processed = (
                item.strip()
                .replace('+', '')
                .replace(' ', '')
                .replace('$', '')
                .replace('€', '')
                .replace('£', '')
                .replace(',', '.')
            )
            
            # Проверка на пустое значение
            if not processed:
                cleaned.append("N/A")
                continue
                
            # Проверка числового формата
            if any(c.isalpha() for c in processed):
                cleaned.append("N/A")
                continue
                
            cleaned.append(processed)
        except Exception as e:
            logging.error(f"Ошибка очистки значения {item}: {str(e)}")
            cleaned.append("ERROR")
    return cleaned

class GoogleSheetsManager:
    """Менеджер для работы с Google Sheets"""
    def __init__(self):
        self.gc = None
        self.last_request_time = 0

    async def connect(self):
        """Подключение к Google Sheets"""
        encoded_creds = os.getenv('GOOGLE_CREDENTIALS_BASE64')
        if not encoded_creds:
            raise ValueError("GOOGLE_CREDENTIALS_BASE64 not set")

        with open(CONFIG["CREDS_FILE"], 'w') as f:
            f.write(base64.b64decode(encoded_creds).decode('utf-8'))

        scope = [
            'https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive'
        ]
        creds = ServiceAccountCredentials.from_json_keyfile_name(
            CONFIG["CREDS_FILE"], scope
        )
        self.gc = gspread.authorize(creds)

    async def safe_get_urls(self):
        """Безопасное получение URL из таблицы"""
        await self._wait_for_api_limit()
        sheet = self.gc.open_by_key(CONFIG["SPREADSHEET_ID"]).worksheet(CONFIG["SHEET_NAME"])
        return sheet.get(f"C{CONFIG['START_ROW']}:C{CONFIG['START_ROW'] + CONFIG['TOTAL_URLS']}")

    async def safe_batch_update(self, updates):
        """Безопасное пакетное обновление данных"""
        await self._wait_for_api_limit()
        sheet = self.gc.open_by_key(CONFIG["SPREADSHEET_ID"]).worksheet(CONFIG["SHEET_NAME"])
        try:
            return sheet.batch_update(updates)
        except Exception as e:
            logging.error(f"Ошибка обновления: {str(e)}")
            return None

    async def _wait_for_api_limit(self):
        """Ожидание соблюдения лимитов API"""
        elapsed = time.time() - self.last_request_time
        if elapsed < CONFIG["API_DELAY"]:
            wait_time = CONFIG["API_DELAY"] - elapsed
            await asyncio.sleep(wait_time)
        self.last_request_time = time.time()

async def parse_url(url, browser, attempt=1):
    """Парсинг URL с улучшенной обработкой ошибок"""
    page = None
    try:
        page = await browser.new_page()
        await page.goto(url, wait_until="domcontentloaded", timeout=30000)
        await asyncio.sleep(random.uniform(1.0, 2.5))

        result = {}
        for col, selectors in CONFIG["TARGET_CLASSES"].items():
            result[col] = ["N/A"]
            for selector in selectors:
                try:
                    elements = await page.query_selector_all(f'.{selector}')
                    if elements:
                        texts = [await el.inner_text() for el in elements]
                        cleaned = [t.strip() for t in texts if t.strip()]
                        
                        # Логирование найденных значений
                        logging.debug(f"Найдены значения для {col}: {cleaned[:3]}")
                        
                        if cleaned and not any(is_error_value(v) for v in cleaned[:3]):
                            result[col] = cleaned[:3]
                            break
                except Exception as e:
                    logging.warning(f"Ошибка парсинга {selector}: {str(e)}")
                    continue
        return result

    except Exception as e:
        logging.error(f"Попытка {attempt} ошибка: {str(e)}")
        if attempt < CONFIG["MAX_RETRIES"]:
            await asyncio.sleep(CONFIG["REQUEST_DELAY"] * attempt)
            return await parse_url(url, browser, attempt + 1)
        return {col: ["FAIL"] for col in CONFIG["TARGET_CLASSES"]}
    finally:
        if page:
            await page.close()

async def process_batch(batch, browser):
    """Обработка пакета URL с улучшенным логированием"""
    tasks = []
    url_map = {}
    
    # Первичная обработка
    for row, url in batch:
        task = asyncio.create_task(parse_url(url, browser))
        tasks.append(task)
        url_map[task] = (row, url)
    
    results = await asyncio.gather(*tasks)
    
    # Сбор и логирование неудачных результатов
    retry_tasks = []
    retry_map = {}
    for task, (row, url) in url_map.items():
        result = task.result()
        error_columns = [col for col, vals in result.items() if any(is_error_value(v) for v in vals)]
        if error_columns:
            logging.warning(f"Повторная обработка строки {row} (ошибки в колонках: {', '.join(error_columns)})")
            retry_task = asyncio.create_task(parse_url(url, browser))
            retry_tasks.append(retry_task)
            retry_map[retry_task] = (row, url)
    
    # Повторная обработка
    if retry_tasks:
        retry_results = await asyncio.gather(*retry_tasks)
        for retry_task, (row, url) in retry_map.items():
            result = retry_task.result()
            results.append((row, result))
            logging.info(f"Результат повтора для строки {row}: {result}")
    
    return list(zip([r[0] for r in batch], results))

async def main():
    """Основная функция выполнения"""
    setup_logging()
    sheets = GoogleSheetsManager()
    browser, playwright = None, None
    
    try:
        # Инициализация подключений
        await sheets.connect()
        browser, playwright = await setup_browser()

        # Получение URL
        urls_data = await sheets.safe_get_urls()
        valid_urls = [
            (CONFIG["START_ROW"] + i, row[0])
            for i, row in enumerate(urls_data)
            if row and row[0].startswith('http')
        ]

        # Обработка блоками
        for i in range(0, len(valid_urls), CONFIG["MAX_CONCURRENT_PAGES"]):
            batch = valid_urls[i:i + CONFIG["MAX_CONCURRENT_PAGES"]]
            logging.info(f"Обработка блока {i//CONFIG['MAX_CONCURRENT_PAGES'] + 1}")
            
            results = await process_batch(batch, browser)
            
            # Подготовка обновлений
            updates = []
            error_count = 0
            for row, result in results:
                try:
                    # Очистка и валидация данных
                    col_d = clean_numeric_values(result.get('col_d', [])[:3])
                    col_e = clean_numeric_values(result.get('col_e', [])[:3])
                    col_f = clean_numeric_values(result.get('col_f', [])[:3])
                    
                    # Проверка качества данных
                    valid_data = True
                    for vals in [col_d, col_e, col_f]:
                        if any(v in CONFIG["ERROR_VALUES"] for v in vals):
                            valid_data = False
                            break
                    
                    if valid_data:
                        updates.append({
                            'range': f'D{row}:G{row}',
                            'values': [[
                                ', '.join(col_d),
                                ', '.join(col_e),
                                ', '.join(col_f),
                                datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            ]]
                        })
                    else:
                        error_count += 1
                        logging.warning(f"Строка {row} содержит невалидные данные")
                except Exception as e:
                    logging.error(f"Ошибка обработки строки {row}: {str(e)}")
                    error_count += 1
            
            # Пакетное обновление
            if updates:
                success = await sheets.safe_batch_update(updates)
                if success:
                    logging.info(f"Успешно обновлено {len(updates)} строк")
                else:
                    logging.error("Не удалось выполнить обновление")
            
            logging.info(f"Блок {i//CONFIG['MAX_CONCURRENT_PAGES'] + 1} завершен. Ошибок: {error_count}")
            
            await asyncio.sleep(random.randint(5, 15))

    except Exception as e:
        logging.critical(f"Критическая ошибка: {str(e)}", exc_info=True)
    finally:
        # Очистка ресурсов
        if browser:
            await browser.close()
        if playwright:
            await playwright.stop()
        if os.path.exists(CONFIG["CREDS_FILE"]):
            os.remove(CONFIG["CREDS_FILE"])

if __name__ == "__main__":
    asyncio.run(main())
