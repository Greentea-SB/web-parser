import os
import base64
import gspread
import logging
import time
import random
from datetime import datetime
from playwright.sync_api import sync_playwright
from typing import Dict, List, Optional

class ParserConfig:
    """Конфигурация для различных вариантов структуры сайта"""
    URL_COLUMN = 3  # Колонка C
    TARGETS = {
        'D': [
            ['css-16udrhy'],
            ['css-sahmrr', 'css-kavdos', 'css-1598eja'],
            ['css-j4xe5q', 'css-d865bw', 'css-krr03m']
        ],
        'E': [
            ['css-16udrhy'],
            ['css-1598eja'],
            ['css-d865bw']
        ],
        'F': [
            ['css-16udrhy'],
            ['css-kavdos'],
            ['css-krr03m']
        ]
    }
    TIMEOUTS = {
        'page_load': 45000,
        'element': 15000,
        'retry_delay': lambda attempt: 10 * (attempt + 1)
    }

class DynamicParser:
    def __init__(self):
        self.browser = None
        self.class_cache: Dict[str, Dict[str, str]] = {}

    def init_browser(self, playwright):
        """Инициализация браузера с расширенными настройками"""
        self.browser = playwright.chromium.launch(
            headless=True,
            args=[
                '--no-sandbox',
                '--disable-setuid-sandbox',
                '--disable-dev-shm-usage',
                '--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            ]
        )
    
    def parse_page(self, url: str, domain: str) -> Dict[str, Optional[List[str]]]:
        """Основная функция парсинга с автоматическим определением структуры"""
        result = {col: None for col in ParserConfig.TARGETS.keys()}
        
        if domain not in self.class_cache:
            self.class_cache[domain] = {}

        for attempt in range(3):
            try:
                page = self.browser.new_page()
                page.set_default_timeout(ParserConfig.TIMEOUTS['page_load'])
                
                # Загрузка страницы с рандомизированными действиями
                page.goto(url, wait_until="networkidle")
                self.human_like_interaction(page)
                
                # Определение актуальных классов для домена
                self.detect_classes(page, domain)
                
                # Сбор данных с кешированных классов
                for col in ParserConfig.TARGETS.keys():
                    result[col] = self.safe_extract(
                        page, 
                        self.class_cache[domain].get(col, ParserConfig.TARGETS[col][0]),
                        col
                    )
                
                return result

            except Exception as e:
                logging.error(f"Attempt {attempt+1} failed: {str(e)}")
                time.sleep(ParserConfig.TIMEOUTS['retry_delay'](attempt))
            finally:
                if page:
                    page.close()
        
        return result

    def detect_classes(self, page, domain: str):
        """Автоматическое определение рабочих классов для домена"""
        for col, variants in ParserConfig.TARGETS.items():
            if col in self.class_cache[domain]:
                continue
                
            for class_group in variants:
                for class_name in class_group:
                    if page.query_selector(f'.{class_name}'):
                        self.class_cache[domain][col] = class_name
                        logging.info(f"Detected class {class_name} for {domain} column {col}")
                        break
                if col in self.class_cache[domain]:
                    break

    def safe_extract(self, page, selector: str, col: str) -> List[str]:
        """Безопасное извлечение данных с резервными стратегиями"""
        try:
            elements = page.query_selector_all(f'.{selector}')
            return [el.inner_text().strip() for el in elements if el]
        except:
            logging.warning(f"Failed to extract {col} using {selector}")
            return self.fallback_extract(page, col)

    def fallback_extract(self, page, col: str) -> List[str]:
        """Резервные методы извлечения данных"""
        # Стратегия 1: Поиск по текстовым паттернам
        text_patterns = {
            'D': ['price', 'value', 'cost'],
            'E': ['rating', 'score', 'review'],
            'F': ['quantity', 'count', 'stock']
        }
        
        for pattern in text_patterns[col]:
            element = page.query_selector(f'text/{pattern}i')
            if element:
                return [element.inner_text().strip()]
        
        # Стратегия 2: Поиск по структурным признакам
        structural_selectors = {
            'D': 'div:near(:text("Price"))',
            'E': 'span:below(:text("Rating"))',
            'F': 'td:right-of(:text("Stock"))'
        }
        
        element = page.query_selector(structural_selectors[col])
        if element:
            return [element.inner_text().strip()]
        
        return ["N/A"]

    def human_like_interaction(self, page):
        """Имитация человеческого поведения"""
        actions = [
            lambda: page.mouse.move(random.randint(0, 500), random.randint(0, 500)),
            lambda: page.keyboard.press("PageDown"),
            lambda: time.sleep(random.uniform(0.5, 2.5)),
            lambda: page.mouse.click(random.randint(0, 500), random.randint(0, 500))
        ]
        random.shuffle(actions)
        [action() for action in actions[:3]]

class GoogleSheetManager:
    def __init__(self, creds_file: str):
        self.gc = gspread.service_account(filename=creds_file)
        self.sheet = None
    
    def init_sheet(self, spreadsheet_id: str, sheet_name: str):
        try:
            spreadsheet = self.gc.open_by_key(spreadsheet_id)
            self.sheet = spreadsheet.worksheet(sheet_name)
        except gspread.exceptions.WorksheetNotFound:
            self.sheet = spreadsheet.add_worksheet(sheet_name, 1000, 26)

    def update_row(self, row: int, data: dict):
        """Обновление строки с автоматическим расширением таблицы"""
        try:
            self.sheet.update(
                f'D{row}:G{row}',
                [[
                    ', '.join(data.get('D', [])),
                    ', '.join(data.get('E', [])),
                    ', '.join(data.get('F', [])),
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                ]],
                value_input_option='USER_ENTERED'
            )
        except gspread.exceptions.APIError as e:
            self.handle_api_error(e, row)

    def handle_api_error(self, error, row: int):
        """Обработка специфичных ошибок Google Sheets API"""
        if "exceeds grid limits" in str(error):
            self.sheet.add_rows(100)
            self.update_row(row)
        else:
            logging.error(f"Sheet API Error: {str(error)}")
            self.sheet.update_cell(row, 8, f"API Error: {str(error)}")

def main():
    """Главная управляющая функция"""
    try:
        # Инициализация
        parser = DynamicParser()
        sheet_manager = GoogleSheetManager("temp_creds.json")
        
        # Конфигурация
        encoded_creds = base64.b64decode(os.getenv('GOOGLE_CREDENTIALS_BASE64')).decode()
        with open("temp_creds.json", "w") as f:
            f.write(encoded_creds)
        
        sheet_manager.init_sheet(
            "1loVjBMvaO-Ia5JnzMTz8YaGqq10XDz-L1LRWNDDVzsE",
            "pars"
        )

        with sync_playwright() as playwright:
            parser.init_browser(playwright)
            
            for row in range(14, 514):  # Строки 14-513
                try:
                    url = sheet_manager.sheet.cell(row, ParserConfig.URL_COLUMN).value
                    if not url or not url.startswith('http'):
                        continue
                        
                    domain = url.split('//')[-1].split('/')[0]
                    result = parser.parse_page(url, domain)
                    sheet_manager.update_row(row, result)
                    
                    time.sleep(random.expovariate(1/3))  # Экспоненциальное распределение задержек

                except Exception as e:
                    logging.error(f"Row {row} processing failed: {str(e)}")
                    sheet_manager.sheet.update_cell(row, 8, f"Processing Error: {str(e)}")

    finally:
        if os.path.exists("temp_creds.json"):
            os.remove("temp_creds.json")

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler("smart_parser.log"),
            logging.StreamHandler()
        ]
    )
    main()
