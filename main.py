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
    "MAX_NA_RETRIES": 5,          # Количество повторов при невалидном результате
    "REQUEST_DELAY": 5,           # Задержка между запросами
    "MAX_CONCURRENT_PAGES": 25,   # Максимальное число одновременных страниц
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
    """Проверяет, что все полученные значения корректны (не содержат ошибок)."""
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
    Если ни один селектор не дал результата – возвращает "N/A" для столбца.
    """
    page = await browser.new_page()
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=15000)
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
                        break  # Если нашли по одному селектору, переходим к следующему столбцу
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
    Повторяет запрос до MAX_NA_RETRIES раз с увеличением задержки.
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

async def process_urls(urls, browser):
    """Обрабатывает список URL с ограничением по количеству одновременных страниц."""
    semaphore = asyncio.Semaphore(CONFIG["MAX_CONCURRENT_PAGES"])

    async def limited_fetch(url):
        async with semaphore:
            return await fetch_url(url, browser)

    return await asyncio.gather(*(limited_fetch(url) for url in urls))

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

        # Обработка URL пакетами
        for batch_start in range(0, CONFIG["TOTAL_URLS"], CONFIG["MAX_CONCURRENT_PAGES"]):
            current_row = CONFIG["START_ROW"] + batch_start
            # Чтение URL из столбца C
            cells = sheet.range(f"C{current_row}:C{current_row + CONFIG['MAX_CONCURRENT_PAGES'] - 1}")
            urls = [cell.value.strip() for cell in cells if cell.value and cell.value.startswith("http")]
            if not urls:
                continue

            results = await process_urls(urls, browser)

            # Подготовка данных для записи в столбцы D, E, F
            update_data = []
            for res in results:
                row = [
                    ", ".join(clean_numeric_values(res.get("col_d", []))[:3]),
                    ", ".join(clean_numeric_values(res.get("col_e", []))[:3]),
                    ", ".join(clean_numeric_values(res.get("col_f", []))[:3])
                ]
                update_data.append(row)

            if update_data:
                sheet.update(
                    f"D{current_row}:F{current_row + len(update_data) - 1}",
                    update_data,
                    value_input_option="USER_ENTERED"
                )

            # Задержка между пакетами
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
