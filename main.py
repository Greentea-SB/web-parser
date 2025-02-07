import base64
import gspread
import logging
import time
import random
import os
from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials
from playwright.sync_api import sync_playwright
from googleapiclient.errors import HttpError

# Настройка логгера
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("parser.log"),
        logging.StreamHandler()
    ]
)

class GoogleSheetsManager:
    def __init__(self, config):
        self.config = config
        self.service = None
        self.sheet = None
        self.batch_buffer = []
        self.last_update = time.time()
        
    def connect(self):
        """Инициализация подключения к Google Sheets"""
        try:
            # Декодирование учетных данных
            encoded_creds = os.getenv('GOOGLE_CREDENTIALS_BASE64')
            if not encoded_creds:
                raise ValueError("GOOGLE_CREDENTIALS_BASE64 не установлена")

            # Создание временного файла с учетными данными
            with open(self.config["CREDS_FILE"], 'w') as f:
                f.write(base64.b64decode(encoded_creds).decode('utf-8'))

            # Авторизация
            scope = [
                'https://spreadsheets.google.com/feeds',
                'https://www.googleapis.com/auth/drive'
            ]
            gc = gspread.authorize(
                ServiceAccountCredentials.from_json_keyfile_name(
                    self.config["CREDS_FILE"], 
                    scope
                )
            )
            
            self.sheet = gc.open_by_key(self.config["SPREADSHEET_ID"]).worksheet(self.config["SHEET_NAME"])
            logging.info("Успешное подключение к Google Sheets")
            
        except Exception as e:
            logging.critical(f"Ошибка подключения: {str(e)}")
            raise

    def safe_update(self, data):
        """
        Безопасное обновление данных с обработкой квот
        """
        backoff_time = 1
        max_retries = 5
        
        for attempt in range(max_retries):
            try:
                if time.time() - self.last_update > 60:  # Лимит 1 запрос в секунду
                    self.sheet.batch_update(data)
                    self.last_update = time.time()
                    return True
                else:
                    time.sleep(1)
                    continue
                    
            except HttpError as e:
                if e.resp.status == 429:
                    sleep_time = backoff_time * (2 ** attempt)
                    logging.warning(f"Превышена квота. Попытка {attempt+1} через {sleep_time} сек.")
                    time.sleep(sleep_time + random.uniform(0, 1))
                else:
                    raise
                    
            except Exception as e:
                logging.error(f"Ошибка обновления: {str(e)}")
                return False
                
        logging.error("Не удалось выполнить обновление после нескольких попыток")
        return False

class Parser:
    def __init__(self, config):
        self.config = config
        self.browser = None
        self.sheets = GoogleSheetsManager(config)
        
    def __enter__(self):
        self.sheets.connect()
        playwright = sync_playwright().start()
        self.browser = playwright.chromium.launch(
            headless=True,
            args=[
                '--no-sandbox',
                '--disable-setuid-sandbox',
                '--disable-web-security',
                '--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            ]
        )
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.browser:
            self.browser.close()
        if os.path.exists(self.config["CREDS_FILE"]):
            os.remove(self.config["CREDS_FILE"])
            
    def process_batch(self, urls):
        """Обработка пакета URL"""
        results = []
        for row_num, url in urls:
            try:
                data = self.parse_url(url)
                results.append({
                    'row': row_num,
                    'data': data,
                    'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                })
                time.sleep(random.uniform(1, 3))
            except Exception as e:
                logging.error(f"Ошибка обработки {url}: {str(e)}")
        return results
        
    def parse_url(self, url):
        """Парсинг одного URL"""
        page = None
        try:
            page = self.browser.new_page()
            page.set_default_timeout(30000)
            page.goto(url, wait_until="domcontentloaded")
            
            # Имитация человеческого поведения
            self.human_like_actions(page)
            
            # Сбор данных
            result = {}
            for col, selectors in self.config["TARGET_CLASSES"].items():
                result[col] = self.find_data(page, selectors)
                
            return result
            
        finally:
            if page:
                page.close()
                
    def human_like_actions(self, page):
        """Имитация действий пользователя"""
        time.sleep(random.uniform(1, 2))
        page.mouse.move(
            random.randint(0, 500),
            random.randint(0, 500)
        )
        page.keyboard.down('PageDown')
        time.sleep(random.uniform(0.5, 1.5))
        page.keyboard.up('PageDown')
        
    def find_data(self, page, selectors):
        """Поиск данных с несколькими попытками"""
        for selector in selectors:
            try:
                elements = page.query_selector_all(f'.{selector}')
                if elements:
                    return [el.inner_text().strip() for el in elements][:3]
            except:
                continue
        return ["N/A"]
        
    def clean_data(self, value):
        """Очистка числовых значений"""
        return (
            value.strip()
            .replace('+', '')
            .replace(' ', '')
            .replace('$', '')
            .replace(',', '.')
        )

def main():
    config = {
        "SPREADSHEET_ID": "1loVjBMvaO-Ia5JnzMTz8YaGqq10XDz-L1LRWNDDVzsE",
        "SHEET_NAME": "pars",
        "CREDS_FILE": "temp_key.json",
        "START_ROW": 14,
        "TOTAL_URLS": 260,
        "BATCH_SIZE": 10,
        "TARGET_CLASSES": {
            'col_d': ['css-16udrhy', 'css-nd24it'],
            'col_e': ['css-sahmrr', 'css-1598eja'],
            'col_f': ['css-j4xe5q', 'css-krr03m']
        }
    }
    
    try:
        with Parser(config) as parser:
            # Получение списка URL
            rows = range(config["START_ROW"], config["START_ROW"] + config["TOTAL_URLS"])
            urls = [
                (row, parser.sheets.sheet.cell(row, 3).value)
                for row in rows
            ]
            
            # Фильтрация и разбиение на пакеты
            valid_urls = [(r, u) for r, u in urls if u and u.startswith('http')]
            batches = [valid_urls[i:i+config["BATCH_SIZE"]] 
                      for i in range(0, len(valid_urls), config["BATCH_SIZE"])]
            
            for batch in batches:
                results = parser.process_batch(batch)
                
                # Подготовка данных для записи
                updates = []
                for item in results:
                    row = item['row']
                    data = item['data']
                    updates.append({
                        'range': f'D{row}:G{row}',
                        'values': [[
                            ', '.join([parser.clean_data(v) for v in data['col_d']]),
                            ', '.join([parser.clean_data(v) for v in data['col_e']]),
                            ', '.join([parser.clean_data(v) for v in data['col_f']]),
                            item['timestamp']
                        ]]
                    })
                
                # Пакетное обновление
                if updates:
                    parser.sheets.safe_update({'requests': updates})
                    logging.info(f"Обновлено {len(updates)} строк")
                
                # Задержка между пакетами
                time.sleep(random.randint(10, 20))
                
    except Exception as e:
        logging.critical(f"Критическая ошибка: {str(e)}")

if __name__ == "__main__":
    main()
