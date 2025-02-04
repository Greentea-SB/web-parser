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
    "REQUEST_DELAY": 15,
    "START_ROW": 14,
    "TOTAL_URLS": 500,
    "TARGET_CLASSES": {
        'col_d': ['css-16udrhy', 'css-16udrhy', 'css-16udrhy'],
        'col_e': ['css-sahmrr', 'css-kavdos', 'css-1598eja'],
        'col_f': ['css-j4xe5q', 'css-d865bw', 'css-krr03m']
    }
}

def clean_numeric_values(data_list):
    """Очистка числовых значений для корректной обработки в Google Sheets"""
    cleaned = []
    for item in data_list:
        try:
            # Удаляем нечисловые символы и нормализуем формат
            processed = (
                item.strip()
                .replace('+', '')
            )
            
            # Пробуем преобразовать к числу
            num = float(processed)
            
            # Форматируем без экспоненты и лишних нулей
            if num.is_integer():
                cleaned.append(str(int(num)))
            else:
                cleaned.append(f"{num:.2f}".rstrip('0').rstrip('.'))
        except:
            cleaned.append(item)
    return cleaned

def parse_data(url, browser):
    # ... остальная часть функции parse_data без изменений ...

def main():
    try:
        # ... код инициализации Google Sheets ...

        with sync_playwright() as playwright:
            browser = setup_browser(playwright)
            
            for i in range(CONFIG["TOTAL_URLS"]):
                row = CONFIG["START_ROW"] + i
                try:
                    # ... получение URL и парсинг данных ...

                    # Подготовка данных с очисткой числовых значений
                    values = [
                        ', '.join(clean_numeric_values(result['col_d'][:3])),
                        ', '.join(clean_numeric_values(result['col_e'][:3])),
                        ', '.join(clean_numeric_values(result['col_f'][:3])),
                        datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    ]

                    # ... обновление таблицы ...

                except Exception as e:
                    # ... обработка ошибок ...

    # ... остальная часть кода без изменений ...

if __name__ == "__main__":
    # ... инициализация логгера ...
    main()
