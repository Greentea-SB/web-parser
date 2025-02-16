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
    "BATCH_SIZE": 5,
    "START_ROW": 14,
    "TOTAL_URLS": 260,
    "TARGET_CLASSES": {
        'col_d': ['css-16udrhy', 'css-16udrhy', 'css-nd24it'],
        'col_e': ['css-sahmrr', 'css-kavdos', 'css-1598eja'],
        'col_f': ['css-j4xe5q', 'css-d865bw', 'css-krr03m']
    },
    "MAX_CONCURRENT_BROWSERS": 3,
    "NAVIGATION_TIMEOUT": 60000,
    "WAIT_TIMEOUT": 10000,
    "SHEETS_API_DELAY": 2  # Задержка между запросами к API
}

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
]

PROXIES = []

# ... (оставляем все вспомогательные функции без изменений: is_valid_number, clean_numeric_values, extract_value, extract_pnl_values)

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

class SheetManager:
    def __init__(self, sheet):
        self.sheet = sheet
        self.last_request_time = 0
        
    async def get_range(self, range_name):
        await self._wait_for_rate_limit()
        return self.sheet.get(range_name)
    
    async def update_range(self, range_name, values):
        await self._wait_for_rate_limit()
        self.sheet.update(range_name=range_name, values=values, value_input_option='RAW')
    
    async def get_batch_urls(self, start_row, batch_size):
        await self._wait_for_rate_limit()
        range_name = f'C{start_row}:C{start_row + batch_size - 1}'
        values = self.sheet.get(range_name)
        return [val[0] if val else None for val in values] if values else []
    
    async def _wait_for_rate_limit(self):
        current_time = time.time()
        time_since_last_request = current_time - self.last_request_time
        if time_since_last_request < CONFIG["SHEETS_API_DELAY"]:
            await asyncio.sleep(CONFIG["SHEETS_API_DELAY"] - time_since_last_request)
        self.last_request_time = time.time()

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
                
                try:
                    urls = await sheet_manager.get_batch_urls(start_row, CONFIG["BATCH_SIZE"])
                    
                    if not urls:
                        continue

                    logger.info(f"Processing batch of {len(urls)} URLs starting at row {start_row}")
                    values = await process_batch(urls, browser)

                    if values:
                        range_name = f'D{start_row}:M{start_row + len(values) - 1}'
                        logger.info(f"Updating range {range_name}")
                        await sheet_manager.update_range(range_name, values)
                        logger.info(f"Updated {len(values)} rows")

                    # Добавляем задержку между батчами
                    await asyncio.sleep(CONFIG["SHEETS_API_DELAY"])

                except Exception as e:
                    logger.error(f"Error processing batch starting at row {start_row}: {e}")
                    # Добавляем дополнительную задержку при ошибке
                    await asyncio.sleep(CONFIG["SHEETS_API_DELAY"] * 2)
                    continue

            await browser.close()

    except Exception as e:
        logger.critical(f"Critical error: {str(e)}", exc_info=True)
    finally:
        if os.path.exists(CONFIG["CREDS_FILE"]):
            os.remove(CONFIG["CREDS_FILE"])

if __name__ == "__main__":
    asyncio.run(main())
