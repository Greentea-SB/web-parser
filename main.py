import asyncio
import base64
import gspread
import logging
import os
import random
import time
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
    browser = await playwright.chromium.launch(headless=True, args=[
        '--no-sandbox',
        '--disable-setuid-sandbox',
        '--disable-web-security',
        '--disable-features=IsolateOrigins,site-per-process',
        '--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    ])
    return browser, playwright

async def parse_data(url, browser):
    """Парсинг одной страницы с асинхронными запросами"""
    for attempt in range(CONFIG["MAX_RETRIES"]):
        page = await browser.new_page()
        try:
            await page.goto(url, wait_until="domcontentloaded")
            await asyncio.sleep(random.uniform(1.0, 2.5))

            results = {col: ["N/A"] for col in CONFIG["TARGET_CLASSES"]}
            for col, selectors in CONFIG["TARGET_CLASSES"].items():
                for selector in selectors:
                    try:
                        await page.wait_for_selector(f'.{selector}', timeout=5000)
                        elements = await page.query_selector_all(f'.{selector}')
                        if elements:
                            results[col] = [await el.inner_text() for el in elements]
                            break
                    except:
                        pass

            # Проверяем, если все данные "N/A", то это неудачный парсинг
            if all(len(results[col]) == 1 and results[col][0] == "N/A" for col in CONFIG["TARGET_CLASSES"]):
                raise Exception("Page data not found")
            
            return results
        except:
            await asyncio.sleep(CONFIG["REQUEST_DELAY"] * (attempt + 1))
        finally:
            await page.close()

    return {col: ["FAIL"] for col in CONFIG["TARGET_CLASSES"]}

async def process_urls(urls, browser):
    """Асинхронный парсинг группы URL"""
    tasks = [parse_data(url, browser) for url in urls]
    return await asyncio.gather(*tasks)

async def retry_failed_urls(failed_urls, browser, max_retries):
    """Повторный парсинг только неудачных страниц"""
    retries = {url: 0 for url in failed_urls}
    
    while failed_urls and max(retries.values()) < max_retries:
        logging.info(f"Retrying {len(failed_urls)} failed URLs...")
        tasks = [parse_data(url, browser) for url in failed_urls]
        new_results = await asyncio.gather(*tasks)
        
        updated_failed_urls = []
        for i, url in enumerate(failed_urls):
            result = new_results[i]
            if all(len(result[col]) == 1 and result[col][0] in ["N/A", "FAIL"] for col in CONFIG["TARGET_CLASSES"]):
                retries[url] += 1
                if retries[url] < max_retries:
                    updated_failed_urls.append(url)
            else:
                failed_urls.remove(url)

        failed_urls = updated_failed_urls
        await asyncio.sleep(CONFIG["REQUEST_DELAY"])  # Задержка перед повтором

    return failed_urls

async def main():
    """Основная асинхронная функция"""
    try:
        # Декодирование Google Credentials
        encoded_creds = os.getenv('GOOGLE_CREDENTIALS_BASE64')
        if not encoded_creds:
            raise ValueError("GOOGLE_CREDENTIALS_BASE64 not set")
        with open(CONFIG["CREDS_FILE"], 'w') as f:
            f.write(base64.b64decode(encoded_creds).decode('utf-8'))
        
        # Авторизация Google Sheets
        gc = gspread.authorize(ServiceAccountCredentials.from_json_keyfile_name(CONFIG["CREDS_FILE"], [
            'https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive'
        ]))
        sheet = gc.open_by_key(CONFIG["SPREADSHEET_ID"]).worksheet(CONFIG["SHEET_NAME"])

        # Запуск браузера
        browser, playwright = await setup_browser()

        for i in range(0, CONFIG["TOTAL_URLS"], CONFIG["MAX_CONCURRENT_PAGES"]):
            start = CONFIG["START_ROW"] + i
            urls = [sheet.cell(start + j, 3).value for j in range(CONFIG["MAX_CONCURRENT_PAGES"])]
            urls = [url for url in urls if url and url.startswith('http')]
            if not urls:
                continue

            # Парсим сразу `MAX_CONCURRENT_PAGES` страниц параллельно
            results_list = await process_urls(urls, browser)

            # Отфильтруем неудачные страницы
            failed_urls = [urls[j] for j, res in enumerate(results_list) if all(
                len(res[col]) == 1 and res[col][0] in ["N/A", "FAIL"] for col in CONFIG["TARGET_CLASSES"]
            )]

            if failed_urls:
                failed_urls = await retry_failed_urls(failed_urls, browser, CONFIG["MAX_NA_RETRIES"])

            # Подготовка данных для записи в Google Sheets
            values = [
                [
                    ', '.join(clean_numeric_values(results['col_d'][:3])),
                    ', '.join(clean_numeric_values(results['col_e'][:3])),
                    ', '.join(clean_numeric_values(results['col_f'][:3]))
                ]
                for results in results_list
            ]
            sheet.update(range_name=f'D{start}:F{start + len(values) - 1}', values=values, value_input_option='USER_ENTERED')

            await asyncio.sleep(random.uniform(3, 7))

        # Закрываем браузер
        await browser.close()
        await playwright.stop()
    except Exception as e:
        logging.critical(f"Critical error: {str(e)}")
    finally:
        if os.path.exists(CONFIG["CREDS_FILE"]):
            os.remove(CONFIG["CREDS_FILE"])

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", handlers=[logging.FileHandler("parser.log"), logging.StreamHandler()])
    asyncio.run(main())
