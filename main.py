import asyncio
import base64
import gspread
import logging
import os
import random
import re
import time
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
    "REQUEST_DELAY": 3,
    "PAGE_LOAD_DELAY": 2,
    "MAX_CONCURRENT_PAGES": 10,
    "START_ROW": 14,
    "TOTAL_URLS": 260,
    "TARGET_CLASSES": {
        'col_d': ['css-16udrhy', 'css-16udrhy', 'css-nd24it'],
        'col_e': ['css-sahmrr', 'css-kavdos', 'css-1598eja'],
        'col_f': ['css-j4xe5q', 'css-d865bw', 'css-krr03m']
    },
    "BATCH_SIZE": 20,
    "MAX_PARALLEL_BATCHES": 3,
    "MIN_REQUEST_INTERVAL": 1,
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
    text = text.strip()
    pattern = r'^-?\d+(?:,\d+)*(?:\.\d+)?$'
    return bool(re.match(pattern, text))

def clean_numeric_values(data_list):
    cleaned = []
    for item in data_list:
        if isinstance(item, str):
            item = item.strip()
            if item.startswith('+'):
                item = item[1:]
        cleaned.append(item)
    return cleaned

def extract_value(text):
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
    logger.info(f"Raw PnL text: {text}")
    values = ['N/A'] * 7

    try:
        lines = [line.strip() for line in text.split('\n') if line.strip()]
        logger.info(f"Split lines: {lines}")

        # Проверяем на '--'
        if all(line == '--' for line in lines if line not in ['PnL', '7D TXs', '/', 'Total PnL', 'Unrealized Profits', '7D Avg Duration', '7D Total Cost']):
            return values

        # Получаем числа TXs
        for i, line in enumerate(lines):
            if '7D TXs' in line:
                tx_values = []
                j = i + 1
                while j < len(lines) and len(tx_values) < 2:
                    current_line = lines[j].strip()
                    if current_line != '/' and current_line != '--':
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
                if pnl_line != '--':
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
                    if next_line != '--':
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

async def setup_browser():
    logger.info("Setting up browser")
    playwright = await async_playwright().start()
    browser = await playwright.chromium.launch(
        headless=True,
        args=[
            '--no-sandbox',
            '--disable-setuid-sandbox',
            '--disable-web-security',
            '--disable-features=IsolateOrigins,site-per-process',
            '--disable-dev-shm-usage',
            '--disable-gpu',
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

        results = {
            'col_d': ["N/A"],
            'col_e': ["N/A"],
            'col_f': ["N/A"],
            'pnl_values': ['N/A'] * 7
        }

        for col in ['col_d', 'col_e', 'col_f']:
            for selector in CONFIG["TARGET_CLASSES"][col]:
                try:
                    element = await page.wait_for_selector(f'.{selector}', timeout=5000)
                    if element:
                        text = await element.inner_text()
                        if text.startswith('+'):
                            text = text[1:]
                        results[col] = [text]
                        break
                except Exception:
                    continue

        try:
            pnl_element = await page.wait_for_selector('.css-1ug9me3', timeout=5000)
            if pnl_element:
                pnl_text = await pnl_element.inner_text()
                if pnl_text:
                    results['pnl_values'] = extract_pnl_values(pnl_text)
        except Exception as e:
            logger.error(f"Error parsing PnL block for {url}: {e}")

        return results

    except Exception as e:
        logger.error(f"Error in parse_data: {e}")
        if error_attempt < CONFIG["MAX_RETRIES"]:
            delay = CONFIG["REQUEST_DELAY"] * (2 ** error_attempt)
            await asyncio.sleep(delay)
            return await parse_data(url, browser, error_attempt + 1)
        return None
    finally:
        await context.close()

async def process_single_url(url, browser):
    for attempt in range(CONFIG["MAX_NA_RETRIES"]):
        result = await parse_data(url, browser)
        if result and any(v != 'N/A' for v in result['pnl_values'][:4]):
            return result
        logger.info(f"Attempt {attempt + 1} failed, retrying after delay...")
        await asyncio.sleep(CONFIG["REQUEST_DELAY"])

    return {
        'col_d': ["N/A"],
        'col_e': ["N/A"],
        'col_f': ["N/A"],
        'pnl_values': ['N/A'] * 7
    }

async def process_urls(urls, browser, request_manager):
    logger.info(f"Processing {len(urls)} URLs")
    
    async def process_url(url):
        if url:
            await request_manager.wait_for_request(url)
            return await process_single_url(url, browser)
        return None

    chunk_size = 5
    chunks = [urls[i:i + chunk_size] for i in range(0, len(urls), chunk_size)]
    
    all_results = []
    for chunk in chunks:
        tasks = [process_url(url) for url in chunk]
        chunk_results = await asyncio.gather(*tasks)
        all_results.extend(chunk_results)
        await asyncio.sleep(1)

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

async def update_sheet(sheet, start_row, values):
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
        logger.info("Connected to Google Sheet")

        browser, playwright = await setup_browser()
        request_manager = RequestManager()
        logger.info("Browser setup complete")

        # Получаем все URL
        all_urls = []
        for i in range(0, CONFIG["TOTAL_URLS"], CONFIG["MAX_CONCURRENT_PAGES"]):
            start_row = CONFIG["START_ROW"] + i
            row_urls = [sheet.cell(start_row + j, 3).value for j in range(CONFIG["MAX_CONCURRENT_PAGES"])]
            row_urls = [url for url in row_urls if url and url.startswith('http')]
            all_urls.extend(row_urls)

        # Обрабатываем URL батчами
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
                    await asyncio.sleep(2)

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
