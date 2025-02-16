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
    "REQUEST_DELAY": 1,  # Уменьшено с 3 до 1
    "PAGE_LOAD_DELAY": 1,  # Уменьшено с 2 до 1
    "MAX_CONCURRENT_PAGES": 20,  # Увеличено с 10 до 20
    "START_ROW": 14,
    "TOTAL_URLS": 260,
    "TARGET_CLASSES": {
        'col_d': ['css-16udrhy', 'css-16udrhy', 'css-nd24it'],
        'col_e': ['css-sahmrr', 'css-kavdos', 'css-1598eja'],
        'col_f': ['css-j4xe5q', 'css-d865bw', 'css-krr03m']
    },
    "BATCH_SIZE": 50,  # Увеличено с 20 до 50
    "MAX_PARALLEL_BATCHES": 5,  # Увеличено с 3 до 5
    "MIN_REQUEST_INTERVAL": 0.5,  # Уменьшено с 1 до 0.5
}

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
]

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

def extract_pnl_values(text):
    values = ['N/A'] * 7
    try:
        lines = [line.strip() for line in text.split('\n') if line.strip()]
        
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

            if 'Total PnL' in line and i + 1 < len(lines):
                pnl_line = lines[i + 1]
                if pnl_line != '--':
                    amount_match = re.search(r'[\+\-]?\$?([\d,.]+[KMB]?)', pnl_line)
                    if amount_match:
                        pnl_value = amount_match.group(1)
                        values[2] = f"-{pnl_value}" if '-' in pnl_line else pnl_value

                    percent_match = re.search(r'\(([-\+]?\d+\.?\d*)%\)', pnl_line)
                    if percent_match:
                        values[3] = f"{percent_match.group(1)}%"

        label_mapping = {
            'Unrealized Profits': 4,
            '7D Avg Duration': 5,
            '7D Total Cost': 6
        }

        for i, line in enumerate(lines):
            for label, index in label_mapping.items():
                if label in line and i + 1 < len(lines):
                    next_line = lines[i + 1].strip()
                    if next_line != '--':
                        values[index] = f"-{next_line}" if next_line.startswith('-') else next_line

        return values
    except Exception as e:
        logger.error(f"Error parsing PnL block: {e}")
        return values

async def setup_browser():
    playwright = await async_playwright().start()
    browser = await playwright.chromium.launch(
        headless=True,
        args=['--no-sandbox', '--disable-setuid-sandbox']
    )
    return browser, playwright

async def parse_data(url, context):
    page = await context.new_page()
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=30000)
        await asyncio.sleep(CONFIG["PAGE_LOAD_DELAY"])

        results = {
            'col_d': ["N/A"],
            'col_e': ["N/A"],
            'col_f': ["N/A"],
            'pnl_values': ['N/A'] * 7
        }

        # Параллельный сбор данных
        tasks = []
        for col in ['col_d', 'col_e', 'col_f']:
            for selector in CONFIG["TARGET_CLASSES"][col]:
                tasks.append(page.wait_for_selector(f'.{selector}', timeout=5000))
        
        elements = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Обработка результатов
        element_index = 0
        for col in ['col_d', 'col_e', 'col_f']:
            for _ in CONFIG["TARGET_CLASSES"][col]:
                element = elements[element_index]
                if isinstance(element, Exception):
                    continue
                if element:
                    text = await element.inner_text()
                    results[col] = [text.lstrip('+')]
                    break
                element_index += 1

        pnl_element = await page.wait_for_selector('.css-1ug9me3', timeout=5000)
        if pnl_element:
            pnl_text = await pnl_element.inner_text()
            if pnl_text:
                results['pnl_values'] = extract_pnl_values(pnl_text)

        return results

    except Exception as e:
        logger.error(f"Error parsing {url}: {e}")
        return None
    finally:
        await page.close()

async def process_batch(urls, browser, request_manager):
    context = await browser.new_context(
        user_agent=random.choice(USER_AGENTS),
        viewport={"width": 1920, "height": 1080}
    )
    
    try:
        tasks = []
        for url in urls:
            if url:
                await request_manager.wait_for_request(url)
                tasks.append(parse_data(url, context))

        results = await asyncio.gather(*tasks)
        
        values = []
        for result in results:
            if result:
                row = [
                    result['col_d'][0],
                    result['col_e'][0],
                    result['col_f'][0],
                    *result['pnl_values']
                ]
                values.append(row)

        return values
    finally:
        await context.close()

async def main():
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

        browser, playwright = await setup_browser()
        request_manager = RequestManager()

        # Получаем все URL одним запросом
        cells = sheet.range(f'C{CONFIG["START_ROW"]}:C{CONFIG["START_ROW"] + CONFIG["TOTAL_URLS"]}')
        urls = [cell.value for cell in cells if cell.value and cell.value.startswith('http')]

        # Обработка в параллельных батчах
        all_values = []
        batch_size = CONFIG["BATCH_SIZE"]
        for i in range(0, len(urls), batch_size):
            batch_urls = urls[i:i + batch_size]
            if not batch_urls:
                continue

            values = await process_batch(batch_urls, browser, request_manager)
            if values:
                start_row = CONFIG["START_ROW"] + i
                range_name = f'D{start_row}:M{start_row + len(values) - 1}'
                sheet.update(range_name, values, value_input_option='RAW')
                all_values.extend(values)

        await browser.close()
        await playwright.stop()

    except Exception as e:
        logger.critical(f"Critical error: {str(e)}", exc_info=True)
    finally:
        if os.path.exists(CONFIG["CREDS_FILE"]):
            os.remove(CONFIG["CREDS_FILE"])

if __name__ == "__main__":
    asyncio.run(main())
