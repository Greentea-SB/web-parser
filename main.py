import os
import base64
import gspread
import logging
import time
import random
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
    "TOTAL_URLS": 5,
    "TARGET_CLASSES": {
        'col_d': 'css-sahmrr',
        'col_e': 'css-1598eja',
        'col_f': 'css-nd24it'
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
    """Улучшенный парсинг с обработкой динамического контента"""
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
                
            # Поиск элементов с расширенной логикой
            results = {}
            for col, selector in CONFIG["TARGET_CLASSES"].items():
                try:
                    page.wait_for_selector(f'.{selector}', timeout=25000)
                    elements = page.query_selector_all(f'.{selector}')
                    results[col] = [el.inner_text().strip() for el in elements]
                except Exception as e:
                    logging.warning(f"Элемент {selector} не найден: {str(e)}")
                    results[col] = ["N/A"]
            
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
        
        gc = gspread.service_account(filename=CONFIG["CREDS_FILE"])
        spreadsheet = gc.open_by_key(CONFIG["SPREADSHEET_ID"])
        sheet = spreadsheet.worksheet(CONFIG["SHEET_NAME"])
        
        # Инициализация браузера
        with sync_playwright() as playwright:
            browser = setup_browser(playwright)
            
            for row in range(CONFIG["START_ROW"], CONFIG["START_ROW"] + CONFIG["TOTAL_URLS"]):
                try:
                    url = sheet.cell(row, 3).value  # Колонка C
                    if not url or not url.startswith('http'):
                        logging.warning(f"Пропуск строки {row}: неверный URL")
                        continue
                        
                    result = parse_data(url, browser)
                    
                    # Подготовка данных
                    values = [
                        ', '.join(result['col_d']),
                        ', '.join(result['col_e']),
                        ', '.join(result['col_f']),
                        datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    ]
                    
                    # Пакетное обновление
                    sheet.update(
                        f'D{row}:G{row}',
                        [values],
                        value_input_option='USER_ENTERED'
                    )
                    
                    logging.info(f"Строка {row} успешно обновлена")
                    time.sleep(random.uniform(2.5, 6.5))
                    
                except Exception as e:
                    logging.error(f"Критическая ошибка в строке {row}: {str(e)}")
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
