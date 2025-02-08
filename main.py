import base64
import gspread
import logging
import time
import random
import os
import asyncio
from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials
from playwright.async_api import async_playwright

CONFIG = {
    "SPREADSHEET_ID": "1loVjBMvaO-Ia5JnzMTz8YaGqq10XDz-L1LRWNDDVzsE",
    "SHEET_NAME": "pars",
    "CREDS_FILE": "temp_key.json",
    "MAX_RETRIES": 3,
    "MAX_NA_RETRIES": 5,
    "REQUEST_DELAY": 15,
    "START_ROW": 14,
    "TOTAL_URLS": 260,
    "TARGET_CLASSES": {
        'col_d': ['css-16udrhy', 'css-16udrhy', 'css-nd24it'],
        'col_e': ['css-sahmrr', 'css-kavdos', 'css-1598eja'],
        'col_f': ['css-j4xe5q', 'css-d865bw', 'css-krr03m']
    }
}

def clean_numeric_values(data_list):
    cleaned = []
    for item in data_list:
        processed = (
            item.strip()
            .replace('+', '')
            .replace(' ', '')
            .replace('$', '')
            .replace('€', '')
            .replace('£', '')
        )
        cleaned.append(processed)
    return cleaned

async def setup_browser(playwright):
    return await playwright.chromium.launch(
        headless=True,
        args=[
            '--no-sandbox',
            '--disable-setuid-sandbox',
            '--disable-web-security',
            '--disable-features=IsolateOrigins,site-per-process',
            '--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        ]
    )

async def human_like_delay(page):
    await asyncio.sleep(random.uniform(1.5, 4.5))
    await page.mouse.move(
        random.randint(0, 500),
        random.randint(0, 500)
    )

async def parse_data(url, browser):
    for attempt in range(CONFIG["MAX_RETRIES"]):
        page = None
        try:
            page = await browser.new_page()
            if page is None:
                raise ValueError("Page could not be created.")
            
            await page.set_default_timeout(60000)
            await page.goto(url, wait_until="domcontentloaded")
            await human_like_delay(page)
            
            results = {}
            for col, selectors in CONFIG["TARGET_CLASSES"].items():
                results[col] = ["N/A"]
                for selector in selectors:
                    try:
                        await page.wait_for_selector(f'.{selector}', timeout=15000)
                        elements = await page.query_selector_all(f'.{selector}')
                        if elements:
                            results[col] = [await el.inner_text().strip() for el in elements]
                            break
                    except Exception as e:
                        logging.debug(f"Selector failed: {str(e)}")
            return results

        except Exception as e:
            logging.error(f"Attempt {attempt + 1} failed: {str(e)}")
            await asyncio.sleep(CONFIG["REQUEST_DELAY"] * (attempt + 1))
        finally:
            if page:
                await page.close()
    
    return {col: ["FAIL"] for col in CONFIG["TARGET_CLASSES"]}

def has_na_values(result):
    return any("N/A" in values for values in result.values())

async def process_row_data(url, browser):
    for na_attempt in range(CONFIG["MAX_NA_RETRIES"]):
        result = await parse_data(url, browser)
        if not has_na_values(result):
            return result
        logging.warning(f"NA retry {na_attempt + 1}")
        await asyncio.sleep(CONFIG["REQUEST_DELAY"] * (na_attempt + 1))
    return result

async def main():
    try:
        encoded_creds = os.getenv('GOOGLE_CREDENTIALS_BASE64')
        if not encoded_creds:
            raise ValueError("GOOGLE_CREDENTIALS_BASE64 not set")

        with open(CONFIG["CREDS_FILE"], 'w') as f:
            f.write(base64.b64decode(encoded_creds).decode('utf-8'))

        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        gc = gspread.authorize(ServiceAccountCredentials.from_json_keyfile_name(CONFIG["CREDS_FILE"], scope))
        sheet = gc.open_by_key(CONFIG["SPREADSHEET_ID"]).worksheet(CONFIG["SHEET_NAME"])

        async with async_playwright() as playwright:
            browser = await setup_browser(playwright)
            
            tasks = []
            for i in range(CONFIG["TOTAL_URLS"]):
                row = CONFIG["START_ROW"] + i
                try:
                    url = sheet.cell(row, 3).value
                    if not url or not url.startswith('http'):
                        continue
                    
                    tasks.append(asyncio.create_task(process_url(url, row, browser, sheet)))
                    
                    if len(tasks) >= 10:  # Ограничение на одновременные запросы
                        await asyncio.gather(*tasks)
                        tasks = []
                        await asyncio.sleep(random.uniform(2.5, 7.5))

                except Exception as e:
                    logging.error(f"Row {row} error: {str(e)}")
                    sheet.update_cell(row, 8, f"ERROR: {str(e)}")
            
            if tasks:
                await asyncio.gather(*tasks)
            
            await browser.close()

    except Exception as e:
        logging.critical(f"Critical error: {str(e)}")
    finally:
        if os.path.exists(CONFIG["CREDS_FILE"]):
            os.remove(CONFIG["CREDS_FILE"])

async def process_url(url, row, browser, sheet):
    result = await process_row_data(url, browser)
                    
    values = [
        ', '.join(clean_numeric_values(result['col_d'][:3])),
        ', '.join(clean_numeric_values(result['col_e'][:3])),
        ', '.join(clean_numeric_values(result['col_f'][:3])),
    ]
    
    sheet.update(
        f'D{row}:G{row}',
        [values],
        value_input_option='USER_ENTERED'
    )

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
