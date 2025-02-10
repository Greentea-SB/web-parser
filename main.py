import asyncio
import base64
import gspread
import logging
import os
import random
from playwright.async_api import async_playwright
from oauth2client.service_account import ServiceAccountCredentials

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
    "TARGET_CLASSES": {
        'col_d': ['css-16udrhy', 'css-16udrhy', 'css-nd24it'],
        'col_e': ['css-sahmrr', 'css-kavdos', 'css-1598eja'],
        'col_f': ['css-j4xe5q', 'css-d865bw', 'css-krr03m']
    }
}

def clean_numeric_values(data_list):
    return [item.strip().replace('+', '').replace(' ', '').replace('$', '').replace('€', '').replace('£', '') 
            for item in data_list if item.strip()]

def is_valid_result(result):
    """Проверяет все значения во всех столбцах на наличие ошибок"""
    error_markers = {"N/A", "--%", "0%", "0", ""}
    for col, values in result.items():
        if not values:
            return False
        for value in values:
            if value.strip() in error_markers:
                return False
    return True

async def setup_browser():
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

async def parse_data(url, browser, error_attempt=1):
    page = await browser.new_page()
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=15000)
        await asyncio.sleep(random.uniform(1.0, 3.0))
        
        results = {}
        for col, selectors in CONFIG["TARGET_CLASSES"].items():
            results[col] = []
            for selector in selectors:
                try:
                    await page.wait_for_selector(f'.{selector}', timeout=5000)
                    elements = await page.query_selector_all(f'.{selector}')
                    if elements:
                        results[col] = [await el.inner_text() for el in elements]
                        break
                except Exception as e:
                    continue
            if not results[col]:
                results[col] = ["N/A"]
                
        return results
    except Exception as e:
        logging.error(f"Error loading {url}: {str(e)}")
        if error_attempt < CONFIG["MAX_RETRIES"]:
            await asyncio.sleep(CONFIG["REQUEST_DELAY"] * error_attempt)
            return await parse_data(url, browser, error_attempt + 1)
        else:
            return {col: ["FAIL"] for col in CONFIG["TARGET_CLASSES"]}
    finally:
        await page.close()

async def process_single_url(url, browser):
    """Обработка URL с повторами при некорректных данных"""
    for na_attempt in range(1, CONFIG["MAX_NA_RETRIES"] + 1):
        result = await parse_data(url, browser)
        logging.info(f"Attempt {na_attempt}/{CONFIG['MAX_NA_RETRIES']} for {url}")
        
        if is_valid_result(result):
            logging.info(f"Valid result for {url}: {result}")
            return result
            
        await asyncio.sleep(CONFIG["REQUEST_DELAY"] * na_attempt)
    
    logging.warning(f"Max attempts reached for {url}. Final result: {result}")
    return result

async def process_urls(urls, browser):
    """Обработка группы URL с ограничением одновременных страниц"""
    semaphore = asyncio.Semaphore(CONFIG["MAX_CONCURRENT_PAGES"])
    
    async def limited_process(url):
        async with semaphore:
            return await process_single_url(url, browser)
    
    return await asyncio.gather(*[limited_process(url) for url in urls])

async def main():
    try:
        # Инициализация Google Sheets
        encoded_creds = os.getenv('GOOGLE_CREDENTIALS_BASE64')
        if not encoded_creds:
            raise ValueError("GOOGLE_CREDENTIALS_BASE64 not set")
        
        with open(CONFIG["CREDS_FILE"], 'w') as f:
            f.write(base64.b64decode(encoded_creds).decode('utf-8'))
            
        scope = ['https://spreadsheets.google.com/feeds',
                'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name(CONFIG["CREDS_FILE"], scope)
        gc = gspread.authorize(creds)
        sheet = gc.open_by_key(CONFIG["SPREADSHEET_ID"]).worksheet(CONFIG["SHEET_NAME"])
        
        # Инициализация браузера
        browser, playwright = await setup_browser()
        
        # Основной цикл обработки
        for batch_start in range(0, CONFIG["TOTAL_URLS"], CONFIG["MAX_CONCURRENT_PAGES"]):
            current_row = CONFIG["START_ROW"] + batch_start
            urls = sheet.range(f'C{current_row}:C{current_row + CONFIG["MAX_CONCURRENT_PAGES"] - 1}')
            urls = [cell.value.strip() for cell in urls if cell.value and cell.value.startswith('http')]
            
            if not urls:
                continue
                
            results = await process_urls(urls, browser)
            
            # Подготовка данных для записи
            update_data = []
            for res in results:
                row = [
                    ', '.join(clean_numeric_values(res.get('col_d', []))[:3]),
                    ', '.join(clean_numeric_values(res.get('col_e', []))[:3]),
                    ', '.join(clean_numeric_values(res.get('col_f', []))[:3])
                ]
                update_data.append(row)
            
            # Пакетное обновление
            if update_data:
                sheet.update(
                    f'D{current_row}:F{current_row + len(update_data) - 1}',
                    update_data,
                    value_input_option='USER_ENTERED'
                )
            
            await asyncio.sleep(random.uniform(3, 7))
            
        await browser.close()
        await playwright.stop()
        
    except Exception as e:
        logging.critical(f"Critical error: {str(e)}", exc_info=True)
    finally:
        if os.path.exists(CONFIG["CREDS_FILE"]):
            os.remove(CONFIG["CREDS_FILE"])

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler("parser.log"),
            logging.StreamHandler()
        ]
    )
    asyncio.run(main())
