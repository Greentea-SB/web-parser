import os
import base64
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from playwright.sync_api import sync_playwright
import time
from datetime import datetime
import logging

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(message)s",
    handlers=[logging.FileHandler("debug.log"), logging.StreamHandler()]
)

# Декодирование ключа из переменной окружения
encoded_creds = os.getenv('GOOGLE_CREDENTIALS_BASE64')
creds_json = base64.b64decode(encoded_creds).decode('utf-8')

# Сохраняем временный файл ключа
with open('temp_key.json', 'w') as f:
    f.write(creds_json)

# Настройки

SPREADSHEET_ID = '1loVjBMvaO-Ia5JnzMTz8YaGqq10XDz-L1LRWNDDVzsE'  # Ваш ID таблицы
SHEET_NAME = 'pars'  # Убедитесь, что это правильное имя листа
CREDS_FILE = 'temp_key.json'  # Временный файл ключа
MAX_RETRIES = 5  # Количество попыток
REQUEST_DELAY = 5  # Задержка между запросами
START_ROW = 14  # Начальная строка с данными
TOTAL_URLS = 500  # Общее количество URL

# Классы для поиска
TARGET_CLASSES = {
    'col_d': 'css-sahmrr',
    'col_e': 'css-1598eja', 
    'col_f': 'css-nd24it'
}

def auth_google():
    """Аутентификация в Google Sheets"""
    scope = [
        'https://spreadsheets.google.com/feeds',
        'https://www.googleapis.com/auth/drive'
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_name(CREDS_FILE, scope)
    return gspread.authorize(creds)

def parse_data(url, browser):
    """Парсинг данных с повторными попытками"""
    for attempt in range(MAX_RETRIES):
        try:
            logging.info(f"Парсинг URL: {url} (попытка {attempt + 1})")
            page = browser.new_page()
            page.goto(url, timeout=60000)
            
            # Ждем загрузки всех элементов
            page.wait_for_selector(f'.{TARGET_CLASSES["col_d"]}', timeout=15000)
            page.wait_for_selector(f'.{TARGET_CLASSES["col_e"]}', timeout=15000)
            page.wait_for_selector(f'.{TARGET_CLASSES["col_f"]}', timeout=15000)
            
            # Извлекаем данные
            result = {
                'd': page.query_selector(f'.{TARGET_CLASSES["col_d"]}').inner_text().strip(),
                'e': page.query_selector(f'.{TARGET_CLASSES["col_e"]}').inner_text().strip(),
                'f': page.query_selector(f'.{TARGET_CLASSES["col_f"]}').inner_text().strip()
            }
            
            page.close()
            logging.info(f"Данные: {result}")
            return result
            
        except Exception as e:
            logging.error(f"Ошибка при парсинге {url}: {str(e)}")
            time.sleep(REQUEST_DELAY)
        finally:
            if 'page' in locals() and not page.is_closed():
                page.close()
    
    return {'d': 'Ошибка', 'e': 'Ошибка', 'f': 'Ошибка'}

def update_sheet(sheet, row, data):
    """Обновление строки в таблице"""
    try:
        sheet.update(
            f'D{row}:F{row}',
            [[data['d'], data['e'], data['f'], datetime.now().strftime("%Y-%m-%d %H:%M:%S")]],
            value_input_option='USER_ENTERED'
        )
        logging.info(f"Обновление строки {row}: {data}")
    except Exception as e:
        logging.error(f"Ошибка при обновлении таблицы: {str(e)}")

def main():
    """Основная функция"""
    gc = auth_google()
    sheet = gc.open_by_key(SPREADSHEET_ID).worksheet(SHEET_NAME)
    
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=['--no-sandbox', '--disable-setuid-sandbox']
        )
        
        for i in range(START_ROW, START_ROW + TOTAL_URLS):
            url = sheet.acell(f'C{i}').value
            
            if not url or not url.startswith('http'):
                logging.warning(f"Строка {i}: Некорректный URL")
                continue
                
            logging.info(f"Обработка строки {i}: {url}")
            result = parse_data(url, browser)
            update_sheet(sheet, i, result)
            
            time.sleep(REQUEST_DELAY)
        
        browser.close()

if __name__ == "__main__":
    main()
