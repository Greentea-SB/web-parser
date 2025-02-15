import asyncio
import base64
import gspread
import logging
import os
import random
import re
from playwright.async_api import async_playwright
from oauth2client.service_account import ServiceAccountCredentials
from concurrent.futures import ThreadPoolExecutor

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
    "REQUEST_DELAY": 2,  # Уменьшено для ускорения
    "PAGE_LOAD_DELAY": 2,  # Уменьшено для ускорения
    "BATCH_SIZE": 10,  # Количество URL для одновременной обработки
    "START_ROW": 14,
    "TOTAL_URLS": 260,
    "TARGET_CLASSES": {
        'col_d': ['css-16udrhy', 'css-16udrhy', 'css-nd24it'],
        'col_e': ['css-sahmrr', 'css-kavdos', 'css-1598eja'],
        'col_f': ['css-j4xe5q', 'css-d865bw', 'css-krr03m']
    },
    "MAX_CONCURRENT_BROWSERS": 5  # Максимальное количество параллельных браузеров
}

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
]

PROXIES = []

# ... (все остальные вспомогательные функции остаются без изменений: is_valid_number, clean_numeric_values, extract_value, extract_pnl_values)

async def setup_browser(playwright):
    """Создает новый браузер"""
    browser = await playwright.chromium.launch(
        headless=True,
        args=[
            '--no-sandbox',
            '--disable-setuid-sandbox',
            '--disable-web-security',
            '--disable-features=IsolateOrigins,site-per-process'
        ]
    )
    return browser

class BrowserPool:
    def __init__(self, playwright, max_browsers):
        self.playwright = playwright
        self.max_browsers = max_browsers
        self.browsers = []
        self.semaphore = asyncio.Semaphore(max_browsers)
        
    async def get_browser(self):
        async with self.semaphore:
            if not self.browsers:
                browser = await setup_browser(self.playwright)
                self.browsers.append(browser)
            return random.choice(self.browsers)
    
    async def cleanup(self):
        for browser in self.browsers:
            await browser.close()
        self.browsers.clear()

async def parse_url_with_retry(url, browser_pool):
    """Парсит один URL с повторными попытками"""
    for attempt in range(CONFIG["MAX_NA_RETRIES"]):
        browser = await browser_pool.get_browser()
        context = await browser.new_context(user_agent=random.choice(USER_AGENTS))
        page = await context.new_page()
        
        try:
            await page.goto(url, wait_until="networkidle")
            await asyncio.sleep(CONFIG["PAGE_LOAD_DELAY"])

            results = {
                'col_d': ["N/A"],
                'col_e': ["N/A"],
                'col_f': ["N/A"],
                'pnl_values': ['N/A'] * 7
            }

            # Парсим базовые колонки
            for col in ['col_d', 'col_e', 'col_f']:
                for selector in CONFIG["TARGET_CLASSES"][col]:
                    try:
                        element = await page.wait_for_selector(f'.{selector}', timeout=10000)
                        if element:
                            text = await element.inner_text()
                            if text.startswith('+'):
                                text = text[1:]
                            results[col] = [text]
                            break
                    except Exception:
                        continue

            # Парсим PnL блок
            try:
                pnl_element = await page.wait_for_selector('.css-1ug9me3', timeout=10000)
                if pnl_element:
                    pnl_text = await pnl_element.inner_text()
                    if pnl_text:
                        results['pnl_values'] = extract_pnl_values(pnl_text)
            except Exception as e:
                logger.error(f"Error parsing PnL block for {url}: {e}")

            if any(v != 'N/A' for v in results['pnl_values'][:4]):
                return results

        except Exception as e:
            logger.error(f"Error processing {url}: {e}")
        finally:
            await context.close()
            
        await asyncio.sleep(CONFIG["REQUEST_DELAY"])
    
    return None

async def process_batch(urls, browser_pool):
    """Обрабатывает пакет URL параллельно"""
    tasks = [parse_url_with_retry(url, browser_pool) for url in urls if url]
    results = await asyncio.gather(*tasks)
    
    values = []
    for result in results:
        if result:
            row_values = [
                ', '.join(clean_numeric_values(result.get('col_d', [])[:3])),
                ', '.join(clean_numeric_values(result.get('col_e', [])[:3])),
                ', '.join(clean_numeric_values(result.get('col_f', [])[:3])),
                *(result.get('pnl_values', ['N/A'] * 7))
            ]
            values.append(row_values)
    
    return values

async def main():
    logger.info("Starting parser")
    try:
        # Инициализация Google Sheets
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

        # Инициализация Playwright и пула браузеров
        async with async_playwright() as playwright:
            browser_pool = BrowserPool(playwright, CONFIG["MAX_CONCURRENT_BROWSERS"])
            
            # Обработка URL батчами
            for i in range(0, CONFIG["TOTAL_URLS"], CONFIG["BATCH_SIZE"]):
                start_row = CONFIG["START_ROW"] + i
                end_row = min(start_row + CONFIG["BATCH_SIZE"], CONFIG["START_ROW"] + CONFIG["TOTAL_URLS"])
                
                # Получение URL для текущего батча
                urls = [sheet.cell(row, 3).value for row in range(start_row, end_row)]
                urls = [url for url in urls if url and url.startswith('http')]
                
                if not urls:
                    continue

                logger.info(f"Processing batch of {len(urls)} URLs starting at row {start_row}")
                values = await process_batch(urls, browser_pool)

                if values:
                    range_name = f'D{start_row}:M{start_row + len(values) - 1}'
                    logger.info(f"Updating range {range_name}")
                    sheet.update(
                        range_name=range_name,
                        values=values,
                        value_input_option='RAW'
                    )
                    logger.info(f"Updated {len(values)} rows")

            # Очистка ресурсов
            await browser_pool.cleanup()

    except Exception as e:
        logger.critical(f"Critical error: {str(e)}", exc_info=True)
    finally:
        if os.path.exists(CONFIG["CREDS_FILE"]):
            os.remove(CONFIG["CREDS_FILE"])

if __name__ == "__main__":
    asyncio.run(main())
