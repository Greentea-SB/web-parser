import base64
import gspread
import logging
import time
import random
import os
from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials
from playwright.sync_api import sync_playwright

# Обновленная конфигурация
CONFIG = {
    "SPREADSHEET_ID": "1loVjBMvaO-Ia5JnzMTz8YaGqq10XDz-L1LRWNDDVzsE",
    "SHEET_NAME": "pars",
    "CREDS_FILE": "temp_key.json",
    "MAX_RETRIES": 3,
    "MAX_NA_RETRIES": 5,  # Добавлено максимальное количество попыток для N/A
    "REQUEST_DELAY": 15,
    "START_ROW": 14,
    "TOTAL_URLS": 500,
    "TARGET_CLASSES": {
        'col_d': ['css-16udrhy', 'css-16udrhy', 'css-16udrhy'],
        'col_e': ['css-sahmrr', 'css-kavdos', 'css-1598eja'],
        'col_f': ['css-j4xe5q', 'css-d865bw', 'css-krr03m']
    }
}

def has_na_values(result):
    """Проверка наличия N/A в результатах"""
    return any("N/A" in values for values in result.values())

def process_row_data(url, browser):
    """Обработка данных с повторами при N/A"""
    for na_attempt in range(CONFIG["MAX_NA_RETRIES"]):
        result = parse_data(url, browser)
        if not has_na_values(result):
            return result
        logging.warning(f"Попытка {na_attempt + 1}: Обнаружены N/A значения")
        time.sleep(CONFIG["REQUEST_DELAY"] * (na_attempt + 1))
    return result

def parse_data(url, browser):
    """Обновленная функция парсинга"""
    for attempt in range(CONFIG["MAX_RETRIES"]):
        page = None
        try:
            page = browser.new_page()
            page.set_default_timeout(60000)
            page.goto(url, wait_until="domcontentloaded")
            
            # Добавлена дополнительная проверка на загрузку
            page.wait_for_selector('body', timeout=30000)
            
            # Улучшенная обработка динамического контента
            human_like_delay(page)
            
            results = {}
            for col, selectors in CONFIG["TARGET_CLASSES"].items():
                results[col] = ["N/A"]
                for selector in selectors:
                    try:
                        elements = page.query_selector_all(f'.{selector}')
                        if elements:
                            cleaned = [el.inner_text().strip() for el in elements if el.inner_text().strip()]
                            if cleaned:
                                results[col] = cleaned
                                break
                    except Exception as e:
                        logging.debug(f"Ошибка при парсинге {selector}: {str(e)}")
            return results

        except Exception as e:
            logging.error(f"Сетевая ошибка (попытка {attempt + 1}): {str(e)}")
            if attempt == CONFIG["MAX_RETRIES"] - 1:
                return {col: ["NETWORK_ERROR"] for col in CONFIG["TARGET_CLASSES"]}
            time.sleep(CONFIG["REQUEST_DELAY"] * (attempt + 1))
        finally:
            if page:
                page.close()

def main():
    """Обновленная главная функция"""
    try:
        # ... (остальная часть инициализации Google Sheets и браузера без изменений)

        with sync_playwright() as playwright:
            browser = setup_browser(playwright)
            
            for i in range(CONFIG["TOTAL_URLS"]):
                row = CONFIG["START_ROW"] + i
                try:
                    url = sheet.cell(row, 3).value
                    if not url or not url.startswith('http'):
                        continue
                    
                    logging.info(f"Начало обработки строки {row}")
                    
                    # Основной цикл обработки с повторами для N/A
                    result = process_row_data(url, browser)
                    
                    # Логирование окончательного результата
                    if has_na_values(result):
                        logging.error(f"Не удалось получить данные после {CONFIG['MAX_NA_RETRIES']} попыток")
                    
                    # ... (остальная часть обработки данных и записи в таблицу)

                except Exception as e:
                    logging.error(f"Критическая ошибка строки {row}: {str(e)}")
                    sheet.update_cell(row, 8, f"FATAL_ERROR: {str(e)}")
                    
    # ... (остальная часть кода без изменений)

if __name__ == "__main__":
    # ... (инициализация логгера без изменений)
    main()
