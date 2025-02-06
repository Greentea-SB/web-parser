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
    "BATCH_SIZE": 25,  # Новый параметр для размера блока
    "TARGET_CLASSES": {
        'col_d': ['css-16udrhy', 'css-16udrhy', 'css-nd24it'],
        'col_e': ['css-sahmrr', 'css-kavdos', 'css-1598eja'],
        'col_f': ['css-j4xe5q', 'css-d865bw', 'css-krr03m']
    }
}

# Остальные функции остаются без изменений до функции main()

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
            
            # Получаем все URL сразу
            all_urls = [
                (CONFIG["START_ROW"] + i, sheet.cell(CONFIG["START_ROW"] + i, 3).value)
                for i in range(CONFIG["TOTAL_URLS"])
            ]
            
            # Фильтруем некорректные URL
            valid_urls = [(row, url) for row, url in all_urls if url and url.startswith('http')]
            total_batches = (len(valid_urls) + CONFIG["BATCH_SIZE"] - 1) // CONFIG["BATCH_SIZE"]
            
            for batch_num in range(total_batches):
                start_idx = batch_num * CONFIG["BATCH_SIZE"]
                end_idx = start_idx + CONFIG["BATCH_SIZE"]
                current_batch = valid_urls[start_idx:end_idx]
                
                remaining_urls = current_batch.copy()
                attempt = 0
                
                while attempt < CONFIG["MAX_RETRIES"] and remaining_urls:
                    logging.info(f"Обработка блока {batch_num+1}, попытка {attempt+1}")
                    failed_urls = []
                    
                    for row, url in remaining_urls:
                        try:
                            result = process_row_data(url, browser)
                            
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
                            
                            logging.info(f"Успешно обработан ряд {row}")
                            
                        except Exception as e:
                            logging.error(f"Ошибка в ряду {row}: {str(e)}")
                            failed_urls.append((row, url))
                            sheet.update_cell(row, 8, f"ERROR: {str(e)}")
                    
                    remaining_urls = failed_urls
                    attempt += 1
                    
                    if remaining_urls:
                        delay = CONFIG["REQUEST_DELAY"] * attempt
                        logging.info(f"Повторная попытка через {delay} секунд...")
                        time.sleep(delay)
                
                if remaining_urls:
                    logging.warning(f"Блок {batch_num+1} содержит неудачные URL: {len(remaining_urls)}")
                
                # Задержка между блоками
                time.sleep(random.uniform(5, 15))
            
            browser.close()

    except Exception as e:
        logging.critical(f"Critical error: {str(e)}")
    finally:
        if os.path.exists(CONFIG["CREDS_FILE"]):
            os.remove(CONFIG["CREDS_FILE"])

# Остальной код без изменений
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler("parser.log"),
            logging.StreamHandler()
        ]
    )
    main()
