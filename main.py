import base64
import gspread
import logging
import time
import random
import os
from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials
from playwright.sync_api import sync_playwright

# Конфигурация
CONFIG = {
    "SPREADSHEET_ID": "1loVjBMvaO-Ia5JnzMTz8YaGqq10XDz-L1LRWNDDVzsE",
    "SHEET_NAME": "pars",
    "CREDS_FILE": "temp_key.json",
    "MAX_RETRIES": 3,
    "REQUEST_DELAY": 15,
    "START_ROW": 14,
    "TOTAL_URLS": 500,
    "TARGET_CLASSES": {
       'col_d': ['css-16udrhy', 'css-16udrhy', 'css-16udrhy'],
        'col_e': ['css-sahmrr', 'css-kavdos', 'css-1598eja'],
        'col_f': ['css-j4xe5q', 'css-d865bw', 'css-krr03m']
    }
}

def setup_browser(playwright):
    """Настройка браузера с использованием playwright объекта"""
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

def handle_captcha(page):
    """Попытка обхода простой капчи"""
    if page.query_selector("text=Verify you are human"):
        logging.warning("Обнаружена капча. Попытка обхода...")
        page.reload()
        human_like_delay(page)
        return True
    return False

def parse_data(url, browser):
    """Улучшенный парсинг с обработкой динамического контента и несколькими вариантами классов"""
    for attempt in range(CONFIG["MAX_RETRIES"]):
        page = None
        try:
            page = browser.new_page()
            page.set_default_timeout(60000)
            
            # Загрузка страницы с эмуляцией человека
            page.goto(url, wait_until="domcontentloaded")
            human_like_delay(page)
            
            # Проверка на капчу
            if handle_captcha(page):
                continue
                
            # Поиск элементов с альтернативными классами
            results = {}
            for col, selectors in CONFIG["TARGET_CLASSES"].items():
                results[col] = ["N/A"]  # Значение по умолчанию
                
                # Перебор всех возможных селекторов для колонки
                for selector in selectors:
                    try:
                        page.wait_for_selector(f'.{selector}', timeout=15000)
                        elements = page.query_selector_all(f'.{selector}')
                        if elements and any(el.inner_text().strip() for el in elements):
                            results[col] = [el.inner_text().strip() for el in elements if el.inner_text().strip()]
                            break  # Успешно нашли элементы
                    except Exception as e:
                        logging.debug(f"Селектор {selector} не сработал: {str(e)}")
                        continue  # Пробуем следующий селектор
                        
            return results
            
        except Exception as e:
            logging.error(f"Попытка {attempt+1} провалена: {str(e)}")
            time.sleep(CONFIG["REQUEST_DELAY"] * (attempt + 1))
        finally:
            if page:
                page.close()
    
    return {col: ["FAIL"] for col in CONFIG["TARGET_CLASSES"]}

def main():
    """Главная функция с расширенной обработкой ошибок"""
    try:
        # Инициализация Google Sheets
        encoded_creds = os.getenv('GOOGLE_CREDENTIALS_BASE64')
        creds_json = base64.b64decode(encoded_creds).decode('utf-8')
        
        with open(CONFIG["CREDS_FILE"], 'w') as f:
            f.write(creds_json)
        
        scope = ['https://spreadsheets.google.com/feeds',
                 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name(CONFIG["CREDS_FILE"], scope)
        gc = gspread.authorize(creds)
        
        spreadsheet = gc.open_by_key(CONFIG["SPREADSHEET_ID"])
        sheet = spreadsheet.worksheet(CONFIG["SHEET_NAME"])
        
        # Инициализация браузера
        with sync_playwright() as playwright:
            browser = setup_browser(playwright)
            
            for i in range(CONFIG["TOTAL_URLS"]):
                row = CONFIG["START_ROW"] + i
                try:
                    url = sheet.cell(row, 3).value  # Колонка C
                    if not url or not url.startswith('http'):
                        logging.warning(f"Пропуск строки {row}: неверный URL")
                        continue
                        
                    logging.info(f"Обработка строки {row}: {url}")
                    result = parse_data(url, browser)
                    
                    # Подготовка данных для записи
                    values = [
                        ', '.join(result['col_d'][:3]),  # Первые 3 элемента
                        ', '.join(result['col_e'][:3]),
                        ', '.join(result['col_f'][:3]),
                        datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    ]
                    
                    # Пакетное обновление
                    sheet.update(
                        f'D{row}:G{row}',
                        [values],
                        value_input_option='USER_ENTERED'
                    )
                    
                    # Случайная задержка между запросами
                    time.sleep(random.uniform(2.5, 7.5))
                    
                except Exception as e:
                    logging.error(f"Ошибка в строке {row}: {str(e)}")
                    sheet.update_cell(row, 8, f"ERROR: {str(e)}")
                    continue
                    
            browser.close()
            
    except Exception as e:
        logging.critical(f"Фатальная ошибка: {str(e)}")
    finally:
        if os.path.exists(CONFIG["CREDS_FILE"]):
            os.remove(CONFIG["CREDS_FILE"])

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
   
