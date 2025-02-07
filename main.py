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
    "MAX_RETRIES": 3,         # Повторы при исключениях (например, ошибка загрузки)
    "MAX_NA_RETRIES": 5,      # Повторы, если результат содержит только "N/A"
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
    return [item.strip().replace('+', '').replace(' ', '').replace('$', '').replace('€', '').replace('£', '') for item in data_list]

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
    """
    Пытаемся загрузить и распарсить данные с URL.
    При исключении повторяем до MAX_RETRIES.
    """
    page = await browser.new_page()
    try:
        await page.goto(url, wait_until="domcontentloaded")
        await asyncio.sleep(random.uniform(1.0, 2.5))
        
        # Изначально для каждого столбца задаём значение "N/A"
        results = {col: ["N/A"] for col in CONFIG["TARGET_CLASSES"]}
        for col, selectors in CONFIG["TARGET_CLASSES"].items():
            for selector in selectors:
                try:
                    await page.wait_for_selector(f'.{selector}', timeout=5000)
                    elements = await page.query_selector_all(f'.{selector}')
                    if elements:
                        results[col] = [await el.inner_text() for el in elements]
                        break  # Если нашли данные для данного столбца, переходим к следующему
                except Exception:
                    continue
        return results
    except Exception as e:
        if error_attempt < CONFIG["MAX_RETRIES"]:
            await asyncio.sleep(CONFIG["REQUEST_DELAY"] * error_attempt)
            return await parse_data(url, browser, error_attempt + 1)
        else:
            return {col: ["FAIL"] for col in CONFIG["TARGET_CLASSES"]}
    finally:
        await page.close()

async def process_single_url(url, browser):
    """
    Парсит один URL с повторными попытками, если данные не найдены.
    Принимает до MAX_NA_RETRIES попыток.
    """
    for na_attempt in range(CONFIG["MAX_NA_RETRIES"]):
        result = await parse_data(url, browser)
        # Если хотя бы для одной колонки данные отличаются от "N/A", считаем, что парсинг успешен
        if not all(len(result[col]) == 1 and result[col][0] == "N/A" for col in CONFIG["TARGET_CLASSES"]):
            return result
        await asyncio.sleep(CONFIG["REQUEST_DELAY"])
    return result  # Если так и не удалось получить данные, результат будет "FAIL" или "N/A"

async def process_urls(urls, browser):
    tasks = [process_single_url(url, browser) for url in urls]
    return await asyncio.gather(*tasks)

async def main():
    try:
        # Декодирование учетных данных Google
        encoded_creds = os.getenv('GOOGLE_CREDENTIALS_BASE64')
        if not encoded_creds:
            raise ValueError("GOOGLE_CREDENTIALS_BASE64 not set")
        with open(CONFIG["CREDS_FILE"], 'w') as f:
            f.write(base64.b64decode(encoded_creds).decode('utf-8'))
        
        # Авторизация и выбор листа в Google Sheets
        gc = gspread.authorize(
            ServiceAccountCredentials.from_json_keyfile_name(
                CONFIG["CREDS_FILE"],
                ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
            )
        )
        sheet = gc.open_by_key(CONFIG["SPREADSHEET_ID"]).worksheet(CONFIG["SHEET_NAME"])
        
        browser, playwright = await setup_browser()
        
        # Обработка URL блоками по MAX_CONCURRENT_PAGES
        for i in range(0, CONFIG["TOTAL_URLS"], CONFIG["MAX_CONCURRENT_PAGES"]):
            start = CONFIG["START_ROW"] + i
            urls = [sheet.cell(start + j, 3).value for j in range(CONFIG["MAX_CONCURRENT_PAGES"])]
            urls = [url for url in urls if url and url.startswith('http')]
            if not urls:
                continue
                
            results_list = await process_urls(urls, browser)
            
            # Подготовка данных для записи в Google Sheets
            values = []
            for res in results_list:
                col_d_val = ', '.join(clean_numeric_values(res.get('col_d', [])[:3]))
                col_e_val = ', '.join(clean_numeric_values(res.get('col_e', [])[:3]))
                col_f_val = ', '.join(clean_numeric_values(res.get('col_f', [])[:3]))
                values.append([col_d_val, col_e_val, col_f_val])
            
            sheet.update(
                range_name=f'D{start}:F{start + len(values) - 1}', 
                values=values, 
                value_input_option='USER_ENTERED'
            )
            
            await asyncio.sleep(random.uniform(3, 7))
            
        await browser.close()
        await playwright.stop()
    except Exception as e:
        logging.critical(f"Critical error: {str(e)}")
    finally:
        if os.path.exists(CONFIG["CREDS_FILE"]):
            os.remove(CONFIG["CREDS_FILE"])

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.FileHandler("parser.log"), logging.StreamHandler()]
    )
    asyncio.run(main())
