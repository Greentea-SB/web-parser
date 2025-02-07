import base64
import gspread
import logging
import time
import random
import os
from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials
from playwright.sync_api import sync_playwright
from concurrent.futures import ThreadPoolExecutor

CONFIG = {
    "SPREADSHEET_ID": "1loVjBMvaO-Ia5JnzMTz8YaGqq10XDz-L1LRWNDDVzsE",
    "SHEET_NAME": "pars",
    "CREDS_FILE": "temp_key.json",
    "MAX_RETRIES": 3,
    "MAX_NA_RETRIES": 5,
    "REQUEST_DELAY": 15,
    "START_ROW": 14,
    "TOTAL_URLS": 260,
    "BLOCK_SIZE": 1,
    "TARGET_CLASSES": {
        'col_d': ['css-16udrhy', 'css-16udrhy', 'css-nd24it'],
        'col_e': ['css-sahmrr', 'css-kavdos', 'css-1598eja'],
        'col_f': ['css-j4xe5q', 'css-d865bw', 'css-krr03m']
    }
}

def clean_numeric_values(data_list):
    return [item.strip().replace('+', '').replace(' ', '').replace('$', '').replace('€', '').replace('£', '') for item in data_list]

def setup_browser(playwright):
    return playwright.chromium.launch(headless=True, args=[
        '--no-sandbox', '--disable-setuid-sandbox', '--disable-web-security',
        '--disable-features=IsolateOrigins,site-per-process',
        '--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    ])

def human_like_delay(page):
    time.sleep(random.uniform(1.5, 4.5))
    page.mouse.move(random.randint(0, 500), random.randint(0, 500))

def parse_data(url, browser):
    for attempt in range(CONFIG["MAX_RETRIES"]):
        page = None
        try:
            page = browser.new_page()
            page.set_default_timeout(60000)
            page.goto(url, wait_until="domcontentloaded")
            human_like_delay(page)

            results = {col: ["N/A"] for col in CONFIG["TARGET_CLASSES"]}
            for col, selectors in CONFIG["TARGET_CLASSES"].items():
                for selector in selectors:
                    try:
                        page.wait_for_selector(f'.{selector}', timeout=15000)
                        elements = page.query_selector_all(f'.{selector}')
                        if elements:
                            results[col] = [el.inner_text().strip() for el in elements]
                            break
                    except Exception:
                        pass
            return results
        except Exception as e:
            logging.error(f"Ошибка парсинга {url}: {str(e)}")
            time.sleep(CONFIG["REQUEST_DELAY"] * (attempt + 1))
        finally:
            if page:
                page.close()
    return {col: ["FAIL"] for col in CONFIG["TARGET_CLASSES"]}

def has_na_values(result):
    return any("N/A" in values for values in result.values())

def process_row_data(url, browser):
    for na_attempt in range(CONFIG["MAX_NA_RETRIES"]):
        result = parse_data(url, browser)
        if not has_na_values(result):
            return result
        logging.warning(f"NA повторная попытка {na_attempt+1} для {url}")
        time.sleep(CONFIG["REQUEST_DELAY"] * (na_attempt + 1))
    return result

def process_block(rows, sheet, browser):
    failed_rows = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(process_row_data, row[1], browser): row for row in rows}
        
        for future in futures:
            row_num, url = futures[future]
            try:
                result = future.result()
                values = [
                    ', '.join(clean_numeric_values(result['col_d'][:3])),
                    ', '.join(clean_numeric_values(result['col_e'][:3])),
                    ', '.join(clean_numeric_values(result['col_f'][:3])),
                ]
                sheet.update(f'D{row_num}:G{row_num}', [values], value_input_option='USER_ENTERED')
            except Exception as e:
                logging.error(f"Ошибка обработки {url}: {str(e)}")
                sheet.update_cell(row_num, 8, f"ERROR: {str(e)}")
                failed_rows.append((row_num, url))

    return failed_rows

def main():
    try:
        encoded_creds = os.getenv('GOOGLE_CREDENTIALS_BASE64')
        if not encoded_creds:
            raise ValueError("GOOGLE_CREDENTIALS_BASE64 не установлен")

        with open(CONFIG["CREDS_FILE"], 'w') as f:
            f.write(base64.b64decode(encoded_creds).decode('utf-8'))

        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        gc = gspread.authorize(ServiceAccountCredentials.from_json_keyfile_name(CONFIG["CREDS_FILE"], scope))
        sheet = gc.open_by_key(CONFIG["SPREADSHEET_ID"]).worksheet(CONFIG["SHEET_NAME"])

        with sync_playwright() as playwright:
            browser = setup_browser(playwright)

            all_urls = []
            for i in range(CONFIG["TOTAL_URLS"]):
                row = CONFIG["START_ROW"] + i
                url = sheet.cell(row, 3).value
                if url and url.startswith('http'):
                    all_urls.append((row, url))

            blocks = [all_urls[i:i + CONFIG["BLOCK_SIZE"]] for i in range(0, len(all_urls), CONFIG["BLOCK_SIZE"])]

            for block in blocks:
                failed_urls = process_block(block, sheet, browser)

                retry_attempts = 0
                while failed_urls and retry_attempts < CONFIG["MAX_NA_RETRIES"]:
                    logging.warning(f"Повторная попытка {retry_attempts+1} для {len(failed_urls)} сайтов")
                    time.sleep(CONFIG["REQUEST_DELAY"])
                    failed_urls = process_block(failed_urls, sheet, browser)
                    retry_attempts += 1

            browser.close()
    except Exception as e:
        logging.critical(f"Критическая ошибка: {str(e)}")
    finally:
        if os.path.exists(CONFIG["CREDS_FILE"]):
            os.remove(CONFIG["CREDS_FILE"])

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s",
                        handlers=[logging.FileHandler("parser.log"), logging.StreamHandler()])
    main()
