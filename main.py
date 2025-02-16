import asyncio
import base64
import gspread
import logging
import os
import random
import re
from playwright.async_api import async_playwright
from oauth2client.service_account import ServiceAccountCredentials

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('parser.log', mode='w', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

CONFIG = {
    "SPREADSHEET_ID": "1loVjBMvaO-Ia5JnzMTz8YaGqq10XDz-L1LRWNDDVzsE",
    "SHEET_NAME": "pars",
    "CREDS_FILE": "temp_key.json",
    "MAX_RETRIES": 3,
    "MAX_NA_RETRIES": 5,
    "REQUEST_DELAY": 3,  # Уменьшено с 10
    "PAGE_LOAD_DELAY": 2,  # Уменьшено с 5
    "MAX_CONCURRENT_PAGES": 10,  # Увеличено с 5
    "START_ROW": 14,
    "TOTAL_URLS": 260,
    "TARGET_CLASSES": {
        'col_d': ['css-16udrhy', 'css-16udrhy', 'css-nd24it'],
        'col_e': ['css-sahmrr', 'css-kavdos', 'css-1598eja'],
        'col_f': ['css-j4xe5q', 'css-d865bw', 'css-krr03m']
    },
    "BATCH_SIZE": 20,  # Размер батча для обновления таблицы
    "MAX_PARALLEL_BATCHES": 3,  # Максимальное количество параллельных батчей
    "MIN_REQUEST_INTERVAL": 1,  # Минимальный интервал между запросами
}

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
]

PROXIES = []

class RequestManager:
    def __init__(self):
        self.last_request_time = {}
        self.lock = asyncio.Lock()

    async def wait_for_request(self, url):
        domain = url.split('/')[2]
        async with self.lock:
            current_time = time.time()
            if domain in self.last_request_time:
                time_since_last = current_time - self.last_request_time[domain]
                if time_since_last < CONFIG["MIN_REQUEST_INTERVAL"]:
                    await asyncio.sleep(CONFIG["MIN_REQUEST_INTERVAL"] - time_since_last)
            self.last_request_time[domain] = time.time()

def is_valid_number(text):
    """Проверяет, является ли текст числом (включая числа с запятыми)"""
    text = text.strip()
    # Паттерн для проверки числа с запятыми, точками или без них
    pattern = r'^-?\d+(?:,\d+)*(?:\.\d+)?$'
    return bool(re.match(pattern, text))

def clean_numeric_values(data_list):
    """Очищает числовые значения от плюсов, сохраняя минусы и запятые"""
    cleaned = []
    for item in data_list:
        if isinstance(item, str):
            item = item.strip()
            if item.startswith('+'):
                item = item[1:]
        cleaned.append(item)
    return cleaned

def extract_value(text):
    """Очищает значение от символов валюты и плюсов, сохраняя минусы и запятые"""
    if not text or text == 'N/A':
        return text
    value = text.strip()
    if value.startswith('+$'):
        value = value[2:]
    elif value.startswith('$'):
        value = value[1:]
    elif value.startswith('+'):
        value = value[1:]
    return value

def extract_pnl_values(text):
    """Извлекает значения из текста PnL с сохранением форматирования"""
    logger.info(f"Raw PnL text: {text}")
    values = ['N/A'] * 7  # [txs1, txs2, total_pnl, pnl_percent, unrealized, duration, total_cost]

    try:
        # Разбиваем текст на строки и удаляем пустые
        lines = [line.strip() for line in text.split('\n') if line.strip()]
        logger.info(f"Split lines: {lines}")

        # Получаем числа TXs
        for i, line in enumerate(lines):
            if '7D TXs' in line:
                tx_values = []
                j = i + 1
                while j < len(lines) and len(tx_values) < 2:
                    current_line = lines[j].strip()
                    if current_line != '/':  # Пропускаем разделитель
                        # Сохраняем числа как есть, включая запятые
                        if re.match(r'^\d+(?:,\d+)*$', current_line):
                            tx_values.append(current_line)
                    j += 1
                if len(tx_values) >= 2:
                    values[0] = tx_values[0]
                    values[1] = tx_values[1]
                break

        # Получаем Total PnL и процент
        for i, line in enumerate(lines):
            if 'Total PnL' in line and i + 1 < len(lines):
                pnl_line = lines[i + 1]
                # Ищем сумму
                amount_match = re.search(r'[\+\-]?\$?([\d,.]+[KMB]?)', pnl_line)
                if amount_match:
                    pnl_value = amount_match.group(1)
                    if '-' in pnl_line and pnl_line.index('-') < pnl_line.index(pnl_value):
                        values[2] = f"-{pnl_value}"
                    else:
                        values[2] = pnl_value

                # Ищем процент
                percent_match = re.search(r'\(([-\+]?\d+\.?\d*)%\)', pnl_line)
                if percent_match:
                    percent_value = percent_match.group(1)
                    if percent_value.startswith('+'):
                        percent_value = percent_value[1:]
                    values[3] = f"{percent_value}%"

        # Словарь соответствия меток и индексов
        label_mapping = {
            'Unrealized Profits': 4,
            '7D Avg Duration': 5,
            '7D Total Cost': 6
        }

        # Получаем остальные значения
        for i, line in enumerate(lines):
            for label, index in label_mapping.items():
                if label in line and i + 1 < len(lines):
                    next_line = lines[i + 1]
                    value = extract_value(next_line)
                    if next_line.startswith('-'):
                        if value.startswith('-'):  # Избегаем двойных минусов
                            values[index] = value
                        else:
                            values[index] = f"-{value}"
                    else:
                        values[index] = value

        logger.info(f"Extracted values: {values}")
        return values

    except Exception as e:
        logger.error(f"Error parsing PnL block: {e}")
        return values

async def setup_browser():
    logger.info("Setting up browser")
    playwright = await async_playwright().start()
    browser = await playwright.chromium.launch(
        headless=True,
        args=[
            '--no-sandbox',
            '--disable-setuid-sandbox',
            '--disable-web-security',
            '--disable-features=IsolateOrigins,site-per-process'
        ]
    )
    return browser, playwright

async def parse_data(url, browser, error_attempt=1):
    logger.info(f"Parsing URL: {url}")
    context_args = {
        "user_agent": random.choice(USER_AGENTS),
        "viewport": {"width": 1920, "height": 1080}
    }
    if PROXIES:
        context_args["proxy"] = {"server": random.choice(PROXIES)}

    context = await browser.new_context(**context_args)
    page = await context.new_page()

    try:
        response = await page.goto(url, wait_until="domcontentloaded", timeout=30000)
        if response.status >= 400:
            logger.error(f"HTTP error {response.status} for URL: {url}")
            return None
            
        await asyncio.sleep(CONFIG["PAGE_LOAD_DELAY"])

        # ... (остальной код parse_data остается прежним)

    except Exception as e:
        logger.error(f"Error in parse_data: {e}")
        if error_attempt < CONFIG["MAX_RETRIES"]:
            delay = CONFIG["REQUEST_DELAY"] * (2 ** error_attempt)  # Экспоненциальная задержка
            await asyncio.sleep(delay)
            return await parse_data(url, browser, error_attempt + 1)
        return None
    finally:
        await context.close()
async def process_urls(urls, browser, request_manager):
    logger.info(f"Processing {len(urls)} URLs")
    
    async def process_url(url):
        if url:
            await request_manager.wait_for_request(url)
            return await process_single_url(url, browser)
        return None

    # Разбиваем URLs на меньшие группы для более равномерной нагрузки
    chunk_size = 5
    chunks = [urls[i:i + chunk_size] for i in range(0, len(urls), chunk_size)]
    
    all_results = []
    for chunk in chunks:
        tasks = [process_url(url) for url in chunk]
        chunk_results = await asyncio.gather(*tasks)
        all_results.extend(chunk_results)
        await asyncio.sleep(1)  # Небольшая пауза между чанками

    values = []
    for res in all_results:
        if res:
            row_values = [
                ', '.join(clean_numeric_values(res.get('col_d', [])[:3])),
                ', '.join(clean_numeric_values(res.get('col_e', [])[:3])),
                ', '.join(clean_numeric_values(res.get('col_f', [])[:3])),
                *(res.get('pnl_values', ['N/A'] * 7))
            ]
            logger.info(f"Row values: {row_values}")
            values.append(row_values)

    return values

async def process_urls(urls, browser):
    logger.info(f"Processing {len(urls)} URLs")
    
    # Создаем список задач для асинхронного выполнения
    tasks = []
    for url in urls:
        if url:  # Проверяем, что URL не пустой
            tasks.append(process_single_url(url, browser))
    
    # Запускаем все задачи одновременно
    results = await asyncio.gather(*tasks)
    
    values = []
    for res in results:
        if res:
            row_values = [
                ', '.join(clean_numeric_values(res.get('col_d', [])[:3])),
                ', '.join(clean_numeric_values(res.get('col_e', [])[:3])),
                ', '.join(clean_numeric_values(res.get('col_f', [])[:3])),
                *(res.get('pnl_values', ['N/A'] * 7))
            ]
            logger.info(f"Row values: {row_values}")
            values.append(row_values)

    return values

async def update_sheet(sheet, start_row, values):
    """Отдельная функция для обновления таблицы с повторными попытками"""
    max_retries = 3
    retry_delay = 10
    
    for attempt in range(max_retries):
        try:
            range_name = f'D{start_row}:M{start_row + len(values) - 1}'
            logger.info(f"Updating range {range_name}")
            sheet.update(
                range_name=range_name,
                values=values,
                value_input_option='RAW'
            )
            logger.info(f"Updated {len(values)} rows")
            return True
        except Exception as e:
            logger.error(f"Error updating sheet (attempt {attempt + 1}): {e}")
            if attempt < max_retries - 1:
                await asyncio.sleep(retry_delay)
    return False

async def process_batch(sheet, browser, start_row, batch_size):
    """Обработка одного батча URL"""
    try:
        urls = [sheet.cell(start_row + j, 3).value for j in range(batch_size)]
        urls = [url for url in urls if url and url.startswith('http')]
        
        if not urls:
            logger.info(f"No URLs found starting at row {start_row}")
            return True

        logger.info(f"Processing batch starting at row {start_row}")
        values = await process_urls(urls, browser)

        if values:
            success = await update_sheet(sheet, start_row, values)
            if not success:
                logger.error(f"Failed to update sheet for batch starting at row {start_row}")
                return False

        await asyncio.sleep(CONFIG["REQUEST_DELAY"])
        return True

    except Exception as e:
        logger.error(f"Error processing batch at row {start_row}: {e}")
        return False

async def main():
    logger.info("Starting parser")
    try:
        # ... (код инициализации остается прежним до создания browser)

        request_manager = RequestManager()
        
        # Получаем все URL сразу
        all_urls = []
        for i in range(0, CONFIG["TOTAL_URLS"], CONFIG["MAX_CONCURRENT_PAGES"]):
            start_row = CONFIG["START_ROW"] + i
            urls = [sheet.cell(start_row + j, 3).value for j in range(CONFIG["MAX_CONCURRENT_PAGES"])]
            urls = [url for url in urls if url and url.startswith('http')]
            all_urls.extend(urls)

        # Обрабатываем URL батчами
        results = []
        for i in range(0, len(all_urls), CONFIG["BATCH_SIZE"]):
            batch_urls = all_urls[i:i + CONFIG["BATCH_SIZE"]]
            if not batch_urls:
                continue

            start_row = CONFIG["START_ROW"] + i
            logger.info(f"Processing batch starting at row {start_row}")
            
            batch_values = await process_urls(batch_urls, browser, request_manager)
            
            if batch_values:
                success = await update_sheet(sheet, start_row, batch_values)
                if not success:
                    logger.error(f"Failed to update batch starting at row {start_row}")
                    await asyncio.sleep(10)
                else:
                    logger.info(f"Successfully updated batch starting at row {start_row}")
                    await asyncio.sleep(2)  # Небольшая пауза между обновлениями таблицы

        await browser.close()
        await playwright.stop()
        logger.info("Parser finished successfully")

    except Exception as e:
        logger.critical(f"Critical error: {str(e)}", exc_info=True)
    finally:
        if os.path.exists(CONFIG["CREDS_FILE"]):
            os.remove(CONFIG["CREDS_FILE"])

if __name__ == "__main__":
    asyncio.run(main())
