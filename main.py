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
    "REQUEST_DELAY": 1,
    "PAGE_LOAD_DELAY": 2,
    "MAX_CONCURRENT_PAGES": 20,
    "START_ROW": 14,
    "TOTAL_URLS": 260,
    "TARGET_CLASSES": {
        'col_d': ['css-16udrhy', 'css-16udrhy', 'css-nd24it'],
        'col_e': ['css-sahmrr', 'css-kavdos', 'css-1598eja'],
        'col_f': ['css-j4xe5q', 'css-d865bw', 'css-krr03m']
    },
    "BATCH_SIZE": 20,
    "MAX_PARALLEL_BATCHES": 5,
    "MIN_REQUEST_INTERVAL": 0.5,
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

def clean_numeric_values(data_list):
    cleaned = []
    for item in data_list:
        if isinstance(item, str):
            item = item.strip()
            if item.startswith('+'):
                item = item[1:]
        cleaned.append(item)
    return cleaned

def extract_pnl_values(text):
    values = ['N/A'] * 7
    try:
        lines = [line.strip() for line in text.split('\n') if line.strip()]
        
        # Извлечение TXs
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
                    values[0] = tx_values[0]  # Buy TXs
                    values[1] = tx_values[1]  # Sell TXs

        # Извлечение Total PnL
        for i, line in enumerate(lines):
            if 'Total PnL' in line and i + 1 < len(lines):
                pnl_line = lines[i + 1]
                if pnl_line != '--':
                    # Извлечение суммы
                    amount_match = re.search(r'[\+\-]?\$?([\d,.]+[KMB]?)', pnl_line)
                    if amount_match:
                        pnl_value = amount_match.group(1)
                        values[2] = f"-{pnl_value}" if '-' in pnl_line else pnl_value

                    # Извлечение процента
                    percent_match = re.search(r'\(([-\+]?\d+\.?\d*)%\)', pnl_line)
                    if percent_match:
                        percent_value = percent_match.group(1)
                        values[3] = f"{percent_value}%"

        # Извлечение остальных значений
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
                        if next_line.startswith('$'):
                            next_line = next_line[1:]
                        values[index] = f"-{next_line}" if next_line.startswith('-') else next_line

        return values
    except Exception as e:
        logger.error(f"Error parsing PnL block: {e}")
        return values

async def setup_browser():
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

async def parse_data(url, context):
    page = await context.new_page()
    try:
        await page.goto(url, wait_until="networkidle", timeout=30000)
        await asyncio.sleep(CONFIG["PAGE_LOAD_DELAY"])

        results = {
            'col_d': ["N/A"],
            'col_e': ["N/A"],
            'col_f': ["N/A"],
            'pnl_values': ['N/A'] * 7
        }

        # Ожидание загрузки всех элементов
        await page.wait_for_load_state("networkidle")

        # Получение данных колонок
        for col in ['col_d', 'col_e', 'col_f']:
            for selector in CONFIG["TARGET_CLASSES"][col]:
                try:
                    element = await page.wait_for_selector(f'.{selector}', timeout=10000)
                    if element:
                        text = await element.inner_text()
                        results[col] = [text.lstrip('+')]
                        break
                except Exception:
                    continue

        # Получение PnL данных
        try:
            await page.wait_for_selector('.css-1ug9me3', state="attached", timeout=10000)
            pnl_element = await page.query_selector('.css-1ug9me3')
            if pnl_element:
                pnl_text = await pnl_element.inner_text()
                if pnl_text:
                    results['pnl_values'] = extract_pnl_values(pnl_text)
        except Exception as e:
            logger.error(f"Error getting PnL data for {url}: {e}")

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

        # Получение всех URL
        cells = sheet.range(f'C{CONFIG["START_ROW"]}:C{CONFIG["START_ROW"] + CONFIG["TOTAL_URLS"]}')
        urls = [cell.value for cell in cells if cell.value and cell.value.startswith('http')]

        # Обработка батчами
        for i in range(0, len(urls), CONFIG["BATCH_SIZE"]):
            batch_urls = urls[i:i + CONFIG["BATCH_SIZE"]]
            if not batch_urls:
                continue

            values = await process_batch(batch_urls, browser, request_manager)
            
            if values:
                start_row = CONFIG["START_ROW"] + i
                range_name = f'D{start_row}:M{start_row + len(values) - 1}'
                sheet.update(range_name, values, value_input_option='RAW')
                logger.info(f"Updated rows {start_row} to {start_row + len(values) - 1}")
                await asyncio.sleep(1)  # Небольшая пауза между обновлениями

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
