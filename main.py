import base64
import gspread
import logging
import time
import random
import os
from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials
from playwright.sync_api import sync_playwright

CONFIG = {
    "SPREADSHEET_ID": "1loVjBMvaO-Ia5JnzMTz8YaGqq10XDz-L1LRWNDDVzsE",
    "SHEET_NAME": "pars",
    "CREDS_FILE": "temp_key.json",
    "MAX_RETRIES": 3,
    "MAX_NA_RETRIES": 5,
    "REQUEST_DELAY": 15,
    "START_ROW": 14,
    "TOTAL_URLS": 260,
    "BATCH_SIZE": 25,  # Обновляем по 25 строк за раз
    "TARGET_CLASSES": {
        'col_d': ['css-16udrhy', 'css-16udrhy', 'css-nd24it'],
        'col_e': ['css-sahmrr', 'css-kavdos', 'css-1598eja'],
        'col_f': ['css-j4xe5q', 'css-d865bw', 'css-krr03m']
    }
}

def clean_numeric_values(data_list):
    return [item.strip().replace('+', '').replace(' ', '').replace('$', '').replace('€', '').replace('£', '') for item in data_list]

def setup_browser(playwright):
    return playwright.chromium.launch(
        headless=True,
        args=['--no-sandbox', '--disable-setuid-sandbox']
    )

def parse_data(url, browser):
    for attempt in range(CONFIG["MAX_RETRIES"]):
        page = None
        try:
            page = browser.new_page()
            page.set_default_timeout(60000)
            page.goto(url, wait_until="domcontentloaded")
            time.sleep(random.uniform(1.5, 4.5))  # Имитация человеческих действий
            
            results = {}
            for col, selectors in CONFIG["TARGET_CLASSES"].items():
                results[col] = ["N/A"]
                for selector in selectors:
                    try:
                        elements = page.query_selector_all(f'.{selector}')
                        if elements:
                            results[col] = [el.inner_text().strip() for el in elements]
                            break
                    except Exception as e:
                        logging.debug(f"Selector failed: {str(e)}")
            return results

        except Exception as e:
            logging.error(f"Attempt {attempt+1} failed: {str(e)}")
            time.sleep(CONFIG["REQUEST_DELAY"] * (attempt + 1))
        finally:
            if page:
                page.close()
    
    return {col: ["FAIL"] for col in CONFIG["TARGET_CLASSES"]}

def process_block(urls, browser):
    results = []
    for url in urls:
        result = parse_data(url, browser)
        values = [
            ', '.join(clean_numeric_values(result['col_d'][:3])),
            ', '.join(clean_numeric_values(result['col_e'][:3])),
            ', '.join(clean_numeric_values(result['col_f'][:3])),
        ]
        results.append(values)
        time.sleep(random.uniform(1, 3))  # Задержка перед следующим запросом
    return results

def main():
    try:
        encoded_creds = os.getenv('GOOGLE_CREDENTIALS_BASE64')
        if not encoded_creds:
            raise ValueError("GOOGLE_CREDENTIALS_BASE64 not set")

        with open(CONFIG["CREDS_FILE"], 'w') as f:
            f.write(base64.b64decode(encoded_creds).decode('utf-8'))

        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        gc = gspread.authorize(ServiceAccountCredentials.from_json_keyfile_name(CONFIG["CREDS_FILE"], scope))
        sheet = gc.open_by_key(CONFIG["SPREADSHEET_ID"]).worksheet(CONFIG["SHEET_NAME"])

        with sync_playwright() as playwright:
            browser = setup_browser(playwright)
            
            for start in range(CONFIG["START_ROW"], CONFIG["START_ROW"] + CONFIG["TOTAL_URLS"], CONFIG["BATCH_SIZE"]):
                urls = [sheet.cell(row, 3).value for row in range(start, start + CONFIG["BATCH_SIZE"]) if sheet.cell(row, 3).value]
                
                if not urls:
                    continue

                results = process_block(urls, browser)
                
                sheet.update(f'D{start}:F{start + len(results) - 1}', results, value_input_option='USER_ENTERED')
                time.sleep(random.uniform(5, 10))  # Даем передышку API Google Sheets

            browser.close()

    except Exception as e:
        logging.critical(f"Critical error: {str(e)}")
    finally:
        if os.path.exists(CONFIG["CREDS_FILE"]):
            os.remove(CONFIG["CREDS_FILE"])

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    main()
