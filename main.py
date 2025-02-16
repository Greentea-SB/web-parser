import asyncio
import base64
import gspread
import logging
import os
import random
import re
import time
from playwright.async_api import async_playwright, TimeoutError
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta

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
    "REQUEST_DELAY": 1,
    "PAGE_LOAD_DELAY": 1,
    "BATCH_SIZE": 3,  # Уменьшен размер батча
    "START_ROW": 14,
    "TOTAL_URLS": 260,
    "TARGET_CLASSES": {
        'col_d': ['css-16udrhy', 'css-16udrhy', 'css-nd24it'],
        'col_e': ['css-sahmrr', 'css-kavdos', 'css-1598eja'],
        'col_f': ['css-j4xe5q', 'css-d865bw', 'css-krr03m']
    },
    "MAX_CONCURRENT_BROWSERS": 2,  # Уменьшено количество браузеров
    "NAVIGATION_TIMEOUT": 60000,
    "WAIT_TIMEOUT": 10000,
    "SHEETS_API_DELAY": 3,  # Увеличена задержка между запросами к API
    "BATCH_DELAY": 5,  # Задержка между батчами
    "MAX_REQUESTS_PER_MINUTE": 50  # Максимальное количество запросов в минуту
}

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
]

PROXIES = []

class RateLimiter:
    def __init__(self, max_requests, time_window):
        self.max_requests = max_requests
        self.time_window = time_window
        self.requests = []
        
    async def acquire(self):
        now = datetime.now()
        # Удаляем старые запросы
        self.requests = [req_time for req_time in self.requests 
                        if now - req_time < timedelta(seconds=self.time_window)]
        
        if len(self.requests) >= self.max_requests:
            sleep_time = (self.requests[0] + timedelta(seconds=self.time_window) - now).total_seconds()
            if sleep_time > 0:
                await asyncio.sleep(sleep_time)
                
        self.requests.append(now)

class SheetManager:
    def __init__(self, sheet):
        self.sheet = sheet
        self.last_request_time = 0
        self.rate_limiter = RateLimiter(CONFIG["MAX_REQUESTS_PER_MINUTE"], 60)
        self.cache = {}
        
    async def wait_for_rate_limit(self):
        await self.rate_limiter.acquire()
        current_time = time.time()
        time_since_last_request = current_time - self.last_request_time
        if time_since_last_request < CONFIG["SHEETS_API_DELAY"]:
            await asyncio.sleep(CONFIG["SHEETS_API_DELAY"] - time_since_last_request)
        self.last_request_time = time.time()

    async def get_batch_urls(self, start_row, batch_size):
        cache_key = f"urls_{start_row}_{batch_size}"
        if cache_key in self.cache:
            return self.cache[cache_key]
        
        await self.wait_for_rate_limit()
        try:
            range_name = f'C{start_row}:C{start_row + batch_size - 1}'
            values = self.sheet.get(range_name)
            urls = [val[0] if val else None for val in values] if values else []
            self.cache[cache_key] = urls
            return urls
        except Exception as e:
            logger.error(f"Error getting URLs for range {range_name}: {e}")
            await asyncio.sleep(CONFIG["SHEETS_API_DELAY"] * 2)
            return []

    async def update_range(self, range_name, values):
        await self.wait_for_rate_limit()
        retries = 3
        for attempt in range(retries):
            try:
                self.sheet.update(
                    range_name=range_name,
                    values=values,
                    value_input_option='RAW'
                )
                return
            except Exception as e:
                logger.error(f"Error updating range {range_name} (attempt {attempt + 1}): {e}")
                if attempt < retries - 1:
                    await asyncio.sleep(CONFIG["SHEETS_API_DELAY"] * (attempt + 2))
                else:
                    raise

def is_valid_number(text):
    """Проверяет, является ли текст числом (включая числа с запятыми)"""
    text = text.strip()
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
    values = ['N/A'] * 7

    try:
        lines = [line.strip() for line in text.split('\n') if line.strip()]
        logger.info(f"Split lines: {lines}")

        # Получаем числа TXs
        for i, line in enumerate(lines):
            if '7D TXs' in line:
                tx_values = []
                j = i + 1
                while j < len(lines) and len(tx_values) < 2:
                    current_line = lines[j].strip()
                    if current_line != '/':
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
                amount_match = re.search(r'[\+\-]?\$?([\d,.]+[KMB]?)', pnl_line)
                if amount_match:
                    pnl_value = amount_match.group(1)
                    if '-' in pnl_line and pnl_line.index('-') < pnl_line.index(pnl_value):
                        values[2] = f"-{pnl_value}"
                    else:
                        values[2] = pnl_value

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
                        if value.startswith('-'):
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

async def setup_browser(playwright):
    """Создает новый браузер с оптимизированными настройками"""
    browser = await playwright.chromium.launch(
        headless=True,
        args=[
            '--no-sandbox',
            '--disable-setuid-sandbox',
            '--disable-web-security',
            '--disable-features=IsolateOrigins,site-per-process',
            '--disable-dev-shm-usage',
            '--disable-gpu',
            '--no-first-run',
            '--no-zygote',
            '--single-process'
        ]
    )
    return browser

async def parse_url(url, browser):
    """Парсит один URL"""
    if not url or not url.startswith('http'):
        return None

    context = await browser.new_context(
        user_agent=random.choice(USER_AGENTS),
        viewport={'width': 1920, 'height': 1080}
    )
    page = await context.new_page()
    
    try:
        page.set_default_navigation_timeout(CONFIG["NAVIGATION_TIMEOUT"])
        page.set_default_timeout(CONFIG["WAIT_TIMEOUT"])

        await page.goto(url, wait_until="domcontentloaded")
        await asyncio.sleep(CONFIG["PAGE_LOAD_DELAY"])

        results = {
            'col_d': ["N/A"],
            'col_e': ["N/A"],
            'col_f': ["N/A"],
            'pnl_values': ['N/A'] * 7
        }

        for col in ['col_d', 'col_e', 'col_f']:
            for selector in CONFIG["TARGET_CLASSES"][col]:
                try:
                    element = await page.wait_for_selector(f'.{selector}', timeout=CONFIG["WAIT_TIMEOUT"])
                    if element:
                        text = await element.inner_text()
                        if text.startswith('+'):
                            text = text[1:]
                        results[col] = [text]
                        break
                except Exception:
                    continue

        try:
            pnl_element = await page.wait_for_selector('.css-1ug9me3', timeout=CONFIG["WAIT_TIMEOUT"])
            if pnl_element:
                pnl_text = await pnl_element.inner_text()
                if pnl_text:
                    results['pnl_values'] = extract_pnl_values(pnl_text)
        except Exception as e:
            logger.error(f"Error parsing PnL block for {url}: {e}")

        return results

    except TimeoutError:
        logger.error(f"Timeout error for {url}")
        return None
    except Exception as e:
        logger.error(f"Error processing {url}: {e}")
        return None
    finally:
        await context.close()

async def process_batch(urls, browser):
    """Обрабатывает пакет URL"""
    tasks = []
    for url in urls:
        if url and url.startswith('http'):
            tasks.append(parse_url(url, browser))
            await asyncio.sleep(0.5)

    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    values = []
    for result in results:
        if result and not isinstance(result, Exception):
            row_values = [
                ', '.join(clean_numeric_values(result.get('col_d', [])[:3])),
                ', '.join(clean_numeric_values(result.get('col_e', [])[:3])),
                ', '.join(clean_numeric_values(result.get('col_f', [])[:3])),
                *(result.get('pnl_values', ['N/A'] * 7))
            ]
            values.append(row_values)
        else:
            values.append(['N/A'] * 10)
    
    return values

async def process_with_retries(sheet_manager, browser, start_row):
    """Обрабатывает батч с повторными попытками при ошибках"""
    max_retries = 3
    
    for retry in range(max_retries):
        try:
            urls = await sheet_manager.get_batch_urls(start_row, CONFIG["BATCH_SIZE"])
            if not urls:
                return
                
            logger.info(f"Processing batch of {len(urls)} URLs starting at row {start_row}")
            values = await process_batch(urls, browser)
            
            if values:
                range_name = f'D{start_row}:M{start_row + len(values) - 1}'
                logger.info(f"Updating range {range_name}")
                await sheet_manager.update_range(range_name, values)
                logger.info(f"Updated {len(values)} rows")
            
            # Добавляем задержку между батчами
            await asyncio.sleep(CONFIG["BATCH_DELAY"])
            return True
            
        except Exception as e:
            logger.error(f"Error processing batch at row {start_row} (attempt {retry + 1}): {e}")
            if retry < max_retries - 1:
                await asyncio.sleep(CONFIG["SHEETS_API_DELAY"] * (retry + 2))
            else:
                logger.error(f"Failed to process batch at row {start_row} after {max_retries} attempts")
                return False

async def main():
    logger.info("Starting parser")
    try:
        encoded_creds = os.getenv('GOOGLE_CREDENTIALS_BASE64')
        if not encoded_creds:
            raise ValueError("GOOGLE_CREDENTIALS_BASE64 not set")

        with open(CONFIG["CREDS_FILE"], 'w') as f:
            f.write(base64.b64decode(encoded_creds).decode('utf-8'))

        gc = gspread.authorize(
            ServiceAccountCredentials.from_json_keyfile_name(
                CONFIG["CREDS_FILE"],
                ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
            )
        )
        sheet = gc.open_by_key(CONFIG["SPREADSHEET_ID"]).worksheet(CONFIG["SHEET_NAME"])
        sheet_manager = SheetManager(sheet)
        logger.info("Connected to Google Sheet")

        async with async_playwright() as playwright:
            browser = await setup_browser(playwright)
            
            for i in range(0, CONFIG["TOTAL_URLS"], CONFIG["BATCH_SIZE"]):
                start_row = CONFIG["START_ROW"] + i
                success = await process_with_retries(sheet_manager, browser, start_row)
                
                if not success:
                    logger.warning(f"Skipping to next batch after row {start_row}")
                    await asyncio.sleep(CONFIG["BATCH_DELAY"] * 2)
                    continue

            await browser.close()

    except Exception as e:
        logger.critical(f"Critical error: {str(e)}", exc_info=True)
    finally:
        if os.path.exists(CONFIG["CREDS_FILE"]):
            os.remove(CONFIG["CREDS_FILE"])

if __name__ == "__main__":
    asyncio.run(main())
