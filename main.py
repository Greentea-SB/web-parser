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
    """Очистка числовых значений с сохранением форматирования"""
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
    """Настройка браузера"""
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

def parse_data(url, browser):
    """Логика парсинга (без изменений)"""
    # ... (остальной код функции parse_data из предыдущего ответа)

def main():
    """Главная функция"""
    try:
        # ... (код инициализации Google Sheets и браузера из предыдущего ответа)
        
        # Блок обработки данных
        values = [
            ', '.join(clean_numeric_values(result['col_d'][:3])),
            ', '.join(clean_numeric_values(result['col_e'][:3])),
            ', '.join(clean_numeric_values(result['col_f'][:3])),
            datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        ]
        
        # ... (остальной код main из предыдущего ответа)

if __name__ == "__main__":
   
