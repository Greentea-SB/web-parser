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
    "REQUEST_DELAY": 5,  # Уменьшено с 10
    "PAGE_LOAD_DELAY": 2,  # Уменьшено с 5
    "MAX_CONCURRENT_PAGES": 10,  # Увеличено с 5
    "START_ROW": 24,
    "TOTAL_URLS": 260,
    "TARGET_CLASSES": {
        'col_d': ['css-16udrhy', 'css-16udrhy', 'css-nd24it'],
        'col_e': ['css-sahmrr', 'css-kavdos', 'css-1598eja'],
        'col_f': ['css-j4xe5q', 'css-d865bw', 'css-krr03m']
    },
    "BROWSER_POOL_SIZE": 3,  # Количество параллельных браузеров
    "URLS_PER_BROWSER": 4,  # Количество URL на один браузер
    "SHEETS_BATCH_SIZE": 20,  # Размер батча для обновления таблицы
}

# Добавляем список прокси (замените на реальные прокси)
PROXIES = [
    "http://proxy1:port",
    "http://proxy2:port",
    "http://proxy3:port",
]

class BrowserPool:
    def __init__(self, playwright, size):
        self.playwright = playwright
        self.size = size
        self.browsers = []
        self.current = 0
        self.lock = asyncio.Lock()

    async def initialize(self):
        for _ in range(self.size):
            browser = await self.playwright.chromium.launch(
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
            self.browsers.append(browser)

    async def get_browser(self):
        async with self.lock:
            browser = self.browsers[self.current]
            self.current = (self.current + 1) % self.size
            return browser

    async def close_all(self):
        for browser in self.browsers:
            await browser.close()

class SheetManager:
    def __init__(self, sheet):
        self.sheet = sheet
        self.cache = {}
        self.last_update = 0
        self.update_lock = asyncio.Lock()

    async def get_urls_batch(self, start_row, batch_size):
        cache_key = f"urls_{start_row}_{batch_size}"
        if cache_key in self.cache:
            return self.cache[cache_key]

        range_name = f'C{start_row}:C{start_row + batch_size - 1}'
        urls = self.sheet.get(range_name)
        urls = [url[0] if url else None for url in urls]
        self.cache[cache_key] = urls
        return urls

    async def update_values(self, start_row, values):
        async with self.update_lock:
            current_time = time.time()
            if current_time - self.last_update < 1:  # Минимальный интервал между обновлениями
                await asyncio.sleep(1)
            
            range_name = f'D{start_row}:M{start_row + len(values) - 1}'
            for attempt in range(3):
                try:
                    self.sheet.update(
                        range_name=range_name,
                        values=values,
                        value_input_option='RAW'
                    )
                    self.last_update = time.time()
                    return True
                except Exception as e:
                    logger.error(f"Error updating sheet (attempt {attempt + 1}): {e}")
                    if attempt < 2:
                        await asyncio.sleep(5 * (attempt + 1))
            return False

async def parse_with_browser(url, browser):
    if not url or not url.startswith('http'):
        return None

    proxy = random.choice(PROXIES) if PROXIES else None
    context_options = {
        "user_agent": random.choice(USER_AGENTS),
    }
    if proxy:
        context_options["proxy"] = {"server": proxy}

    context = await browser.new_context(**context_options)
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

        # Парсинг базовых колонок
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

        # Парсинг PnL блока
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
        logger.error(f"Error processing {url}: {e}")
        return None
    finally:
        await context.close()

async def process_url_batch(urls, browser_pool):
    browser = await browser_pool.get_browser()
    tasks = []
    
    for url in urls:
        if url and url.startswith('http'):
            tasks.append(parse_with_browser(url, browser))
    
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
        sheet_manager = SheetManager(sheet)
        logger.info("Connected to Google Sheet")

        async with async_playwright() as playwright:
            # Инициализация пула браузеров
            browser_pool = BrowserPool(playwright, CONFIG["BROWSER_POOL_SIZE"])
            await browser_pool.initialize()
            logger.info("Browser pool initialized")

            # Получаем все URL сразу
            all_urls = await sheet_manager.get_urls_batch(
                CONFIG["START_ROW"],
                CONFIG["TOTAL_URLS"]
            )
            
            # Обработка URL батчами
            tasks = []
            results = []
            
            for i in range(0, len(all_urls), CONFIG["URLS_PER_BROWSER"]):
                batch_urls = all_urls[i:i + CONFIG["URLS_PER_BROWSER"]]
                if not any(batch_urls):
                    continue
                
                # Создаем задачу для батча
                task = process_url_batch(batch_urls, browser_pool)
                tasks.append(task)
                
                # Если накопилось достаточно задач или это последний батч
                if len(tasks) >= CONFIG["BROWSER_POOL_SIZE"] * CONFIG["URLS_PER_BROWSER"] or (i + CONFIG["URLS_PER_BROWSER"]) >= len(all_urls):
                    batch_results = await asyncio.gather(*tasks)
                    results.extend([item for sublist in batch_results for item in sublist])
                    tasks = []
                    
                    # Обновляем таблицу батчами
                    if len(results) >= CONFIG["SHEETS_BATCH_SIZE"] or (i + CONFIG["URLS_PER_BROWSER"]) >= len(all_urls):
                        start_idx = len(results) - len(results) % CONFIG["SHEETS_BATCH_SIZE"]
                        if start_idx > 0:
                            batch_to_update = results[:start_idx]
                            results = results[start_idx:]
                            
                            update_row = CONFIG["START_ROW"] + (len(all_urls) - len(results))
                            success = await sheet_manager.update_values(update_row, batch_to_update)
                            
                            if not success:
                                logger.error(f"Failed to update batch starting at row {update_row}")
                                await asyncio.sleep(10)  # Дополнительная задержка при ошибке
                
                await asyncio.sleep(CONFIG["REQUEST_DELAY"])

            # Закрываем все браузеры
            await browser_pool.close_all()
            logger.info("Parser finished successfully")

    except Exception as e:
        logger.critical(f"Critical error: {str(e)}", exc_info=True)
    finally:
        if os.path.exists(CONFIG["CREDS_FILE"]):
            os.remove(CONFIG["CREDS_FILE"])

if __name__ == "__main__":
    asyncio.run(main())
