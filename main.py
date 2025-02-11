import asyncio
import base64
import os
import random
import logging

import gspread
from oauth2client.service_account import ServiceAccountCredentials
from playwright.async_api import async_playwright

# Основные настройки
CONFIG = {
    "SPREADSHEET_ID": "1loVjBMvaO-Ia5JnzMTz8YaGqq10XDz-L1LRWNDDVzsE",
    "SHEET_NAME": "pars",
    "CREDS_FILE": "temp_key.json",
    "MAX_NA_RETRIES": 5,          # Количество повторных попыток при невалидном результате
    "REQUEST_DELAY": 5,           # Базовая задержка между попытками
    "START_ROW": 14,              # Первая строка с URL
    "TOTAL_URLS": 260,            # Общее число URL для обработки
    "TARGET_CLASSES": {
        "col_d": ["css-16udrhy", "css-nd24it"],
        "col_e": ["css-sahmrr", "css-kavdos", "css-1598eja"],
        "col_f": ["css-j4xe5q", "css-d865bw", "css-krr03m"]
    }
}

def clean_numeric_values(values):
    """Удаляет лишние символы из значений."""
    return [
        v.strip().replace('+', '').replace(' ', '').replace('$', '').replace('€', '').replace('£', '')
        for v in values if v.strip()
    ]

def is_valid_result(result):
    """Проверяет, что все полученные значения корректны (не содержат маркеры ошибок)."""
    if result is None:
        return False
    error_markers = {"N/A", "--%", "0%", "0", ""}
    for vals in result.values():
        if not vals or any(val.strip() in error_markers for val in vals):
            return False
    return True

async def setup_browser():
    """Запускает Playwright и открывает браузер."""
    playwright = await async_playwright().start()
    browser = await playwright.chromium.launch(
        headless=True,
        args=[
            '--no-sandbox',
            '--disable-setuid-sandbox'
        ]
    )
    return browser, playwright

async def fetch_data(url, browser):
    """
    Переход по URL, ожидание загрузки страницы и поиск элементов по заданным селекторам.
    Если ни один селектор не дал результата – возвращается ["N/A"] для соответствующего столбца.
    """
    page = await browser.new_page()
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=15000)
        # Задержка сразу после загрузки страницы
        await asyncio.sleep(random.uniform(1, 3))
        results = {}
        for col, selectors in CONFIG["TARGET_CLASSES"].items():
            results[col] = []
            for selector in selectors:
                try:
                    await page.wait_for_selector(f'.{selector}', timeout=5000)
                    elements = await page.query_selector_all(f'.{selector}')
                    if elements:
                        results[col] = [await el.inner_text() for el in elements]
                        break  # Если нашли данные по одному селектору, переход к следующему столбцу
                except Exception:
                    continue
            if not results[col]:
                results[col] = ["N/A"]
        return results
    except Exception as e:
        logging.error(f"Error fetching {url}: {e}")
        return None
    finally:
        await page.close()

async def fetch_url(url, browser):
    """
    Обрабатывает URL с повторными попытками, если результат невалидный.
    При неудаче ждем CONFIG["REQUEST_DELAY"] * attempt секунд перед следующей попыткой.
    """
    for attempt in range(1, CONFIG["MAX_NA_RETRIES"] + 1):
        logging.info(f"Attempt {attempt}/{CONFIG['MAX_NA_RETRIES']} for {url}")
        result = await fetch_data(url, browser)
        if is_valid_result(result):
            logging.info(f"Valid result for {url}: {result}")
            return result
        await asyncio.sleep(CONFIG["REQUEST_DELAY"] * attempt)
    logging.warning(f"Max attempts reached for {url}. Final result: {result}")
    return result

async def process_urls_sequential(urls, browser):
    """
    Обрабатывает URL-адреса последовательно, с добавлением случайной задержки перед каждым запросом.
    Это гарантирует, что в каждый момент времени обрабатывается только один сайт.
    """
    results = []
    for url in urls:
        # Случайная задержка перед запуском запроса
        await asyncio.sleep(random.uniform(1, 3))
        result = await fetch_url(url, browser)
        results.append(result)
    return results

async def main():
    try:
        # Инициализация Google Sheets
        encoded_creds = os.getenv("GOOGLE_CREDENTIALS_BASE64")
        if not encoded_creds:
            raise ValueError("GOOGLE_CREDENTIALS_BASE64 not set")
        with open(CONFIG["CREDS_FILE"], "w") as f:
            f.write(base64.b64decode(encoded_creds).decode("utf-8"))

        scope = ['https://spreadsheets.google.com/feeds',
                 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name(CONFIG["CREDS_FILE"], scope)
        gc = gspread.authorize(creds)
        sheet = gc.open_by_key(CONFIG["SPREADSHEET_ID"]).worksheet(CONFIG["SHEET_NAME"])

        # Запуск браузера
        browser, playwright = await setup_browser()

        # Обрабатываем URL-адреса последовательно, по одному за раз.
        for row in range(CONFIG["START_ROW"], CONFIG["START_ROW"] + CONFIG["TOTAL_URLS"]):
            cell = sheet.acell(f"C{row}")
            url = cell.value.strip() if cell.value else ""
            if not url or not url.startswith("http"):
                continue

            result = await process_urls_sequential([url], browser)
            # process_urls_sequential возвращает список с одним элементом
            result = result[0]

            # Подготовка данных для записи в столбцы D, E, F
            update_data = [
                ", ".join(clean_numeric_values(result.get("col_d", []))[:3]),
                ", ".join(clean_numeric_values(result.get("col_e", []))[:3]),
                ", ".join(clean_numeric_values(result.get("col_f", []))[:3])
            ]
            # Обновление текущей строки в таблице
            sheet.update(f"D{row}:F{row}", [update_data], value_input_option="USER_ENTERED")

            # Задержка между обработкой URL (от 3 до 7 секунд)
            await asyncio.sleep(random.uniform(3, 7))

        await browser.close()
        await playwright.stop()

    except Exception as e:
        logging.critical(f"Critical error: {e}", exc_info=True)
    finally:
        if os.path.exists(CONFIG["CREDS_FILE"]):
            os.remove(CONFIG["CREDS_FILE"])

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.FileHandler("parser.log"), logging.StreamHandler()]
    )
    asyncio.run(main())
