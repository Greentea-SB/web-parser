import asyncio
import base64
import gspread
import logging
import os
import random
import time
from datetime import datetime
from googleapiclient.errors import HttpError
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
    "MAX_CONCURRENT_PAGES": 25,
    "START_ROW": 14,
    "TOTAL_URLS": 260,
    "ERROR_VALUES": {"0", "--%", "0%", "N/A", "FAIL"},
    "TARGET_CLASSES": {
        'col_d': ['css-16udrhy', 'css-16udrhy', 'css-nd24it'],
        'col_e': ['css-sahmrr', 'css-kavdos', 'css-1598eja'],
        'col_f': ['css-j4xe5q', 'css-d865bw', 'css-krr03m']
    },
    "API_DELAY": 1.2  # Задержка между запросами к Google API
}

def setup_logging():
    """Инициализация системы логирования"""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler("parser.log"),
            logging.StreamHandler()
        ]
    )

def clean_numeric_values(data_list):
    """Очистка числовых значений от нежелательных символов"""
    return [
        item.strip()
        .replace('+', '')
        .replace(' ', '')
        .replace('$', '')
        .replace('€', '')
        .replace('£', '')
        for item in data_list
    ]

async def setup_browser():
    """Инициализация браузера Playwright"""
    playwright = await async_playwright().start()
    browser = await playwright.chromium.launch(
        headless=True,
        args=[
            '--no-sandbox',
            '--disable-setuid-sandbox',
            '--disable-web-security',
            '--disable-features=IsolateOrigins,site-per-process',
            '--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        ]
    )
    return browser, playwright

def is_error_value(value):
    """Проверка на ошибочные значения"""
    return any(err in value for err in CONFIG["ERROR_VALUES"])

class GoogleSheetsManager:
    """Менеджер для работы с Google Sheets с учетом квот"""
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
        return sheet.batch_update(updates)

    async def _wait_for_api_limit(self):
        """Ожидание соблюдения лимитов API"""
        elapsed = time.time() - self.last_request_time
        if elapsed < CONFIG["API_DELAY"]:
            wait_time = CONFIG["API_DELAY"] - elapsed
            await asyncio.sleep(wait_time)
        self.last_request_time = time.time()

async def parse_url(url, browser, attempt=1):
    """Парсинг URL с повторными попытками"""
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
                        if cleaned and not is_error_value(cleaned[0]):
                            result[col] = cleaned[:3]
                            break
                except Exception:
                    continue
        return result

    except Exception as e:
        if attempt < CONFIG["MAX_RETRIES"]:
            await asyncio.sleep(CONFIG["REQUEST_DELAY"] * attempt)
            return await parse_url(url, browser, attempt + 1)
        return {col: ["FAIL"] for col in CONFIG["TARGET_CLASSES"]}
    finally:
        if page:
            await page.close()

async def process_batch(batch, browser):
    """Обработка пакета URL с повторными попытками"""
    tasks = []
    url_map = {}
    
    # Первичная обработка
    for row, url in batch:
        task = asyncio.create_task(parse_url(url, browser))
        tasks.append(task)
        url_map[task] = (row, url)
    
    results = await asyncio.gather(*tasks)
    
    # Повторная обработка неудачных
    retry_tasks = []
    retry_map = {}
    for task, (row, url) in url_map.items():
        result = task.result()
        if any(is_error_value(v[0]) for v in result.values() if v):
            retry_task = asyncio.create_task(parse_url(url, browser))
            retry_tasks.append(retry_task)
            retry_map[retry_task] = (row, url)
    
    if retry_tasks:
        await asyncio.gather(*retry_tasks)
        for retry_task, (row, url) in retry_map.items():
            result = retry_task.result()
            results.append((row, result))
    
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
            for row, result in results:
                try:
                    col_d = ', '.join(clean_numeric_values(result['col_d'][:3]))
                    col_e = ', '.join(clean_numeric_values(result['col_e'][:3]))
                    col_f = ', '.join(clean_numeric_values(result['col_f'][:3]))
                    
                    if not any(is_error_value(v) for v in [col_d, col_e, col_f]):
                        updates.append({
                            'range': f'D{row}:G{row}',
                            'values': [[col_d, col_e, col_f, datetime.now().strftime("%Y-%m-%d %H:%M:%S")]]
                        })
                except Exception as e:
                    logging.error(f"Ошибка обработки строки {row}: {str(e)}")
            
            # Пакетное обновление
            if updates:
                await sheets.safe_batch_update(updates)
                logging.info(f"Обновлено {len(updates)} строк")
            
            await asyncio.sleep(random.randint(5, 15))

    except Exception as e:
        logging.critical(f"Критическая ошибка: {str(e)}")
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
