import os
import base64
import gspread
import logging
import time
from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials
from playwright.sync_api import sync_playwright

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(message)s",
    handlers=[logging.FileHandler("debug.log"), logging.StreamHandler()]
)

# Декодирование ключа
encoded_creds = os.getenv('GOOGLE_CREDENTIALS_BASE64')
creds_json = base64.b64decode(encoded_creds).decode('utf-8')

# Конфигурация
CONFIG = {
    "SPREADSHEET_ID": "1loVjBMvaO-Ia5JnzMTz8YaGqq10XDz-L1LRWNDDVzsE",
    "SHEET_NAME": "pars",
    "CREDS_FILE": "temp_key.json",
    "MAX_RETRIES": 3,
    "REQUEST_DELAY": 10,  # Увеличена задержка
    "START_ROW": 14,
    "TOTAL_URLS": 3,
    "TARGET_CLASSES": {
        'col_d': 'css-sahmrr',
        'col_e': 'css-1598eja',
        'col_f': 'css-nd24it'
    }
}

def auth_google():
    """Аутентификация в Google Sheets"""
    scope = [
        'https://spreadsheets.google.com/feeds',
        'https://www.googleapis.com/auth/drive'
    ]
    with open(CONFIG["CREDS_FILE"], 'w') as f:
        f.write(creds_json)
    
    creds = ServiceAccountCredentials.from_json_keyfile_name(CONFIG["CREDS_FILE"], scope)
    return gspread.authorize(creds)

def safe_parse_element(page, selector):
    """Безопасное извлечение элемента с обработкой ошибок"""
    try:
        element = page.query_selector(selector)
        return element.inner_text().strip() if element else "N/A"
    except Exception as e:
        logging.error(f"Ошибка при парсинге {selector}: {str(e)}")
        return "ERROR"

def parse_data(url, browser):
    """Парсинг данных с улучшенной обработкой ошибок"""
    for attempt in range(CONFIG["MAX_RETRIES"]):
        try:
            logging.info(f"Парсинг URL: {url} (попытка {attempt + 1})")
            page = browser.new_page()
            
            # Настройки браузера
            page.set_default_timeout(45000)  # Увеличенный общий таймаут
            page.goto(url, wait_until="networkidle")
            
            # Ожидание элементов с прогрессивной задержкой
            page.wait_for_selector(f'.{CONFIG["TARGET_CLASSES"]["col_d"]}', 
                                state="attached", 
                                timeout=25000 * (attempt + 1))
            
            return {
                'd': safe_parse_element(page, f'.{CONFIG["TARGET_CLASSES"]["col_d"]}'),
                'e': safe_parse_element(page, f'.{CONFIG["TARGET_CLASSES"]["col_e"]}'),
                'f': safe_parse_element(page, f'.{CONFIG["TARGET_CLASSES"]["col_f"]}')
            }
            
        except Exception as e:
            logging.error(f"Ошибка: {str(e)}")
            time.sleep(CONFIG["REQUEST_DELAY"] * (attempt + 1))
        finally:
            if 'page' in locals() and not page.is_closed():
                page.close()
    
    return {'d': 'FAIL', 'e': 'FAIL', 'f': 'FAIL'}

def update_sheet(sheet, row, data):
    """Обновление таблицы с обработкой ошибок формата"""
    try:
        sheet.update(
            range_name=f'D{row}:G{row}',  # D-G для 4 столбцов
            values=[[
                data['d'], 
                data['e'], 
                data['f'], 
                datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            ]],
            value_input_option='USER_ENTERED'
        )
        logging.info(f"Успешно обновлена строка {row}")
    except Exception as e:
        logging.error(f"Критическая ошибка при обновлении: {str(e)}")

def main():
    """Основная функция с улучшенной обработкой исключений"""
    try:
        gc = auth_google()
        spreadsheet = gc.open_by_key(CONFIG["SPREADSHEET_ID"])
        sheet = spreadsheet.worksheet(CONFIG["SHEET_NAME"])
        
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                args=[
                    '--no-sandbox',
                    '--disable-setuid-sandbox',
                    '--disable-dev-shm-usage'
                ]
            )
            
            for i in range(CONFIG["START_ROW"], CONFIG["START_ROW"] + CONFIG["TOTAL_URLS"]):
                try:
                    url = sheet.acell(f'C{i}').value
                    if not url or not url.startswith('http'):
                        logging.warning(f"Пропуск строки {i}: неверный URL")
                        continue
                        
                    result = parse_data(url, browser)
                    update_sheet(sheet, i, result)
                    time.sleep(CONFIG["REQUEST_DELAY"])
                    
                except Exception as e:
                    logging.error(f"Ошибка обработки строки {i}: {str(e)}")
                    continue
                    
            browser.close()
            
    except Exception as e:
        logging.critical(f"Фатальная ошибка: {str(e)}")
        raise
    finally:
        if os.path.exists(CONFIG["CREDS_FILE"]):
            os.remove(CONFIG["CREDS_FILE"])

if __name__ == "__main__":
    main()
