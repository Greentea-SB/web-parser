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
    "BLOCK_SIZE": 5,
    "START_ROW": 14,
    "TOTAL_URLS": 260,
    "TARGET_CLASSES": {
        'col_d': ['css-16udrhy', 'css-nd24it'],
        'col_e': ['css-sahmrr', 'css-kavdos'],
        'col_f': ['css-j4xe5q', 'css-d865bw']
    }
}

def clean_numeric_values(data_list):
    return [item.strip().replace('+', '').replace(' ', '').replace('$', '').replace('€', '').replace('£', '') for item in data_list]

def setup_browser(playwright):
    return playwright.chromium.launch(headless=True, args=[
        '--no-sandbox',
        '--disable-setuid-sandbox',
        '--disable-web-security',
        '--disable-features=IsolateOrigins,site-per-process',
        '--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    ])

def parse_data(url, browser):
    for attempt in range(CONFIG["MAX_RETRIES"]):
        page = browser.new_page()
        try:
            page.goto(url, wait_until="domcontentloaded")
            time.sleep(random.uniform(1.5, 4.5))
            results = {col: ["N/A"] for col in CONFIG["TARGET_CLASSES"]}
            for col, selectors in CONFIG["TARGET_CLASSES"].items():
                for selector in selectors:
                    try:
                        page.wait_for_selector(f'.{selector}', timeout=15000)
                        elements = page.query_selector_all(f'.{selector}')
                        if elements:
                            results[col] = [el.inner_text().strip() for el in elements]
                            break
                    except:
                        pass
            return results
        except:
            time.sleep(CONFIG["REQUEST_DELAY"] * (attempt + 1))
        finally:
            page.close()
    return {col: ["FAIL"] for col in CONFIG["TARGET_CLASSES"]}

def process_block(urls, browser):
    results = []
    failed_urls = []
    for url in urls:
        result = parse_data(url, browser)
        if any("N/A" in values for values in result.values()):
            failed_urls.append(url)
        results.append([
            ', '.join(clean_numeric_values(result['col_d'][:3])),
            ', '.join(clean_numeric_values(result['col_e'][:3])),
            ', '.join(clean_numeric_values(result['col_f'][:3]))
        ])
    return results, failed_urls

def main():
    try:
        encoded_creds = os.getenv('GOOGLE_CREDENTIALS_BASE64')
        if not encoded_creds:
            raise ValueError("GOOGLE_CREDENTIALS_BASE64 not set")
        with open(CONFIG["CREDS_FILE"], 'w') as f:
            f.write(base64.b64decode(encoded_creds).decode('utf-8'))
        gc = gspread.authorize(ServiceAccountCredentials.from_json_keyfile_name(CONFIG["CREDS_FILE"], [
            'https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive'
        ]))
        sheet = gc.open_by_key(CONFIG["SPREADSHEET_ID"]).worksheet(CONFIG["SHEET_NAME"])
        
        with sync_playwright() as playwright:
            browser = setup_browser(playwright)
            
            for i in range(0, CONFIG["TOTAL_URLS"], CONFIG["BLOCK_SIZE"]):
                start = CONFIG["START_ROW"] + i
                urls = [sheet.cell(start + j, 3).value for j in range(CONFIG["BLOCK_SIZE"])]
                urls = [url for url in urls if url and url.startswith('http')]
                if not urls:
                    continue
                
                results, failed_urls = process_block(urls, browser)
                sheet.update(range_name=f'D{start}:F{start + len(results) - 1}', values=results, value_input_option='USER_ENTERED')
                
                for _ in range(CONFIG["MAX_NA_RETRIES"]):
                    if not failed_urls:
                        break
                    time.sleep(CONFIG["REQUEST_DELAY"])
                    results, failed_urls = process_block(failed_urls, browser)
                    sheet.update(range_name=f'D{start}:F{start + len(results) - 1}', values=results, value_input_option='USER_ENTERED')
                    
            browser.close()
    except Exception as e:
        logging.critical(f"Critical error: {str(e)}")
    finally:
        if os.path.exists(CONFIG["CREDS_FILE"]):
            os.remove(CONFIG["CREDS_FILE"])

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", handlers=[logging.FileHandler("parser.log"), logging.StreamHandler()])
    main()
