import base64
import gspread
import logging
import time
import random
import os
from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials
from playwright.sync_api import sync_playwright

# Настройка логгера
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("parser.log"),
        logging.StreamHandler()
    ]
)

CONFIG = {
    "SPREADSHEET_ID": "1loVjBMvaO-Ia5JnzMTz8YaGqq10XDz-L1LRWNDDVzsE",
    "SHEET_NAME": "pars",
    "CREDS_FILE": "temp_key.json",
    "MAX_RETRIES": 3,
    "MAX_NA_RETRIES": 5,
    "REQUEST_DELAY": 15,
    "START_ROW": 14,
    "TOTAL_URLS": 260,
    "BATCH_SIZE": 25,
    "TARGET_CLASSES": {
        'col_d': ['css-16udrhy', 'css-16udrhy', 'css-nd24it'],
        'col_e': ['css-sahmrr', 'css-kavdos', 'css-1598eja'],
        'col_f': ['css-j4xe5q', 'css-d865bw', 'css-krr03m']
    }
}

def clean_numeric_values(data_list):
    """Очистка числовых значений"""
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

def setup_browser(playwright):
    """Инициализация браузера"""
    return playwright.chromium.launch(
        headless=True,
        args=[
            '--no-sandbox',
            '--disable-setuid-sandbox',
            '--disable-web-security',
            '--disable-features=IsolateOrigins,site-per-process',
            '--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        ]
    )

def human_like_delay(page):
    """Имитация человеческого поведения"""
    time.sleep(random.uniform(1.5, 4.5))
    page.mouse.move(
        random.randint(0, 500),
        random.randint(0, 500)
    )

def parse_data(url, browser):
    """Парсинг данных с сайта"""
    for attempt in range(CONFIG["MAX_RETRIES"]):
        page = None
        try:
            page = browser.new_page()
            page.set_default_timeout(60000)
            page.goto(url, wait_until="domcontentloaded")
            human_like_delay(page)
            
            results = {}
            for col, selectors in CONFIG["TARGET_CLASSES"].items():
                results[col] = ["N/A"]
                for selector in selectors:
                    try:
                        page.wait_for_selector(f'.{selector}', timeout=15000)
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

def has_na_values(result):
    """Проверка наличия N/A значений"""
    return any("N/A" in values for values in result.values())

def process_row_data(url, browser):
    """Обработка данных с повторами"""
    for na_attempt in range(CONFIG["MAX_NA_RETRIES"]):
        result = parse_data(url, browser)
        if not has_na_values(result):
            return result
        logging.warning(f"NA retry {na_attempt+1}")
        time.sleep(CONFIG["REQUEST_DELAY"] * (na_attempt + 1))
    return result

def main():
    """Главная функция"""
    try:
        # Инициализация Google Sheets
        encoded_creds = os.getenv('GOOGLE_CREDENTIALS_BASE64')
        if not encoded_creds:
            raise ValueError("GOOGLE_CREDENTIALS_BASE64 not set")

        with open(CONFIG["CREDS_FILE"], 'w') as f:
            f.write(base64.b64decode(encoded_creds).decode('utf-8'))

        scope = ['https://spreadsheets.google.com/feeds', 
                'https://www.googleapis.com/auth/drive']
        gc = gspread.authorize(
            ServiceAccountCredentials.from_json_keyfile_name(
                CONFIG["CREDS_FILE"], 
                scope
            )
        )
        sheet = gc.open_by_key(CONFIG["SPREADSHEET_ID"]).worksheet(CONFIG["SHEET_NAME"])

        with sync_playwright() as playwright:
            browser = setup_browser(playwright)
            
            # Получаем все URL
            all_urls = [
                (CONFIG["START_ROW"] + i, sheet.cell(CONFIG["START_ROW"] + i, 3).value)
                for i in range(CONFIG["TOTAL_URLS"])
            ]
            
            # Фильтрация URL
            valid_urls = [(row, url) for row, url in all_urls if url and url.startswith('http')]
            total_batches = (len(valid_urls) + CONFIG["BATCH_SIZE"] - 1) // CONFIG["BATCH_SIZE"]
            
            for batch_num in range(total_batches):
                start_idx = batch_num * CONFIG["BATCH_SIZE"]
                end_idx = start_idx + CONFIG["BATCH_SIZE"]
                current_batch = valid_urls[start_idx:end_idx]
                
                remaining_urls = current_batch.copy()
                attempt = 0
                
                while attempt < CONFIG["MAX_RETRIES"] and remaining_urls:
                    logging.info(f"Batch {batch_num+1}, attempt {attempt+1}")
                    failed_urls = []
                    
                    for row, url in remaining_urls:
                        try:
                            result = process_row_data(url, browser)
                            
                            values = [
                                ', '.join(clean_numeric_values(result['col_d'][:3])),
                                ', '.join(clean_numeric_values(result['col_e'][:3])),
                                ', '.join(clean_numeric_values(result['col_f'][:3])),
                                datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            ]
                            
                            sheet.update(
                                f'D{row}:G{row}',
                                [values],
                                value_input_option='USER_ENTERED'
                            )
                            
                            logging.info(f"Row {row} processed successfully")
                            
                        except Exception as e:
                            logging.error(f"Row {row} error: {str(e)}")
                            failed_urls.append((row, url))
                            sheet.update_cell(row, 8, f"ERROR: {str(e)}")
                    
                    remaining_urls = failed_urls
                    attempt += 1
                    
                    if remaining_urls:
                        delay = CONFIG["REQUEST_DELAY"] * attempt
                        logging.info(f"Retrying in {delay} seconds...")
                        time.sleep(delay)
                
                if remaining_urls:
                    logging.warning(f"Batch {batch_num+1} failed URLs: {len(remaining_urls)}")
                
                time.sleep(random.uniform(5, 15))
            
            browser.close()

    except Exception as e:
        logging.critical(f"Critical error: {str(e)}")
    finally:
        if os.path.exists(CONFIG["CREDS_FILE"]):
            os.remove(CONFIG["CREDS_FILE"])

if __name__ == "__main__":
    main()
