import asyncio
import base64
import gspread
import logging
import os
import random
from datetime import datetime
from playwright.async_api import async_playwright
from oauth2client.service_account import ServiceAccountCredentials

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
    }
}

# Настройка логгера
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler("parser.log"), logging.StreamHandler()]
)

def clean_numeric_values(data_list):
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
    return any(err in value for err in CONFIG["ERROR_VALUES"])

async def parse_url(url, browser, attempt=1):
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
    tasks = []
    url_map = {}
    
    # Первоначальная обработка
    for row, url in batch:
        task = asyncio.create_task(parse_url(url, browser))
        tasks.append(task)
        url_map[task] = (row, url)
    
    results = await asyncio.gather(*tasks)
    
    # Сбор неудачных результатов
    retry_tasks = []
    retry_map = {}
    for task, (row, url) in url_map.items():
        result = task.result()
        if any(is_error_value(v[0]) for v in result.values() if v):
            retry_task = asyncio.create_task(parse_url(url, browser))
            retry_tasks.append(retry_task)
            retry_map[retry_task] = (row, url)
    
    # Повторная обработка неудачных
    if retry_tasks:
        retry_results = await asyncio.gather(*retry_tasks)
        for retry_task, (row, url) in retry_map.items():
            result = retry_task.result()
            results.append((row, result))
    
    # Формирование финальных результатов
    final_results = []
    for (row, url), result in zip(batch, results):
        final_results.append((row, result))
    
    return final_results

async def update_sheet(sheet, updates):
    if not updates:
        return
    
    batch = []
    for row, values in updates.items():
        batch.append({
            'range': f'D{row}:G{row}',
            'values': [values + [datetime.now().strftime("%Y-%m-%d %H:%M:%S")]]
        })
    
    try:
        sheet.batch_update(batch)
    except Exception as e:
        logging.error(f"Ошибка обновления таблицы: {str(e)}")

async def main():
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
        
        # Получение всех URL
        all_urls = [
            (CONFIG["START_ROW"] + i, sheet.cell(CONFIG["START_ROW"] + i, 3).value)
            for i in range(CONFIG["TOTAL_URLS"])
        ]
        valid_urls = [(row, url) for row, url in all_urls if url and url.startswith('http')]
        
        browser, playwright = await setup_browser()
        
        # Обработка блоками
        for i in range(0, len(valid_urls), CONFIG["MAX_CONCURRENT_PAGES"]):
            batch = valid_urls[i:i + CONFIG["MAX_CONCURRENT_PAGES"]]
            logging.info(f"Обработка блока {i//CONFIG["MAX_CONCURRENT_PAGES"] + 1}")
            
            results = await process_batch(batch, browser)
            
            # Подготовка данных для обновления
            updates = {}
            for row, result in results:
                col_d = ', '.join(clean_numeric_values(result['col_d'][:3]))
                col_e = ', '.join(clean_numeric_values(result['col_e'][:3]))
                col_f = ', '.join(clean_numeric_values(result['col_f'][:3]))
                
                if not any(is_error_value(v) for v in [col_d, col_e, col_f]):
                    updates[row] = [col_d, col_e, col_f]
            
            # Пакетное обновление
            await update_sheet(sheet, updates)
            await asyncio.sleep(random.randint(5, 10))
        
        await browser.close()
        await playwright.stop()
        
    except Exception as e:
        logging.critical(f"Critical error: {str(e)}")
    finally:
        if os.path.exists(CONFIG["CREDS_FILE"]):
            os.remove(CONFIG["CREDS_FILE"])

if __name__ == "__main__":
    asyncio.run(main())
