import asyncio
import base64
import gspread
import logging
import os
import random
import re
from playwright.async_api import async_playwright
from oauth2client.service_account import ServiceAccountCredentials

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('parser.log', mode='w', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

CONFIG = {
    "SPREADSHEET_ID": "1loVjBMvaO-Ia5JnzMTz8YaGqq10XDz-L1LRWNDDVzsE",
    "SHEET_NAME": "pars",
    "CREDS_FILE": "temp_key.json",
    "MAX_RETRIES": 3,
    "MAX_NA_RETRIES": 5,
    "REQUEST_DELAY": 10,
    "PAGE_LOAD_DELAY": 5,
    "MAX_CONCURRENT_PAGES": 5,
    "START_ROW": 24,
    "TOTAL_URLS": 260,
    "TARGET_CLASSES": {
        'col_d': ['css-16udrhy', 'css-16udrhy', 'css-nd24it'],
        'col_e': ['css-sahmrr', 'css-kavdos', 'css-1598eja'],
        'col_f': ['css-j4xe5q', 'css-d865bw', 'css-krr03m']
    }
}

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
]

PROXIES = []

def is_valid_number(text):
    """Проверяет, является ли текст числом (включая числа с запятыми)"""
    text = text.strip()
    # Паттерн для проверки числа с запятыми, точками или без них
    pattern = r'^-?\d+(?:,\d+)*(?:\.\d+)?$'
    return bool(re.match(pattern, text))

def clean_numeric_values(data_list):
    """Очищает числовые значения от плюсов, сохраняя минусы и запятые"""
    cleaned = []
    for item in data_list:
        if isinstance(item, str):
            item = item.strip()
            if item.startswith('+'):
                item = item[1:]
        cleaned.append(item)
    return cleaned

def extract_value(text):
    """Очищает значение от символов валюты и плюсов, сохраняя минусы и запятые"""
    if not text or text == 'N/A':
        return text
    value = text.strip()
    if value.startswith('+$'):
        value = value[2:]
    elif value.startswith('$'):
        value = value[1:]
    elif value.startswith('+'):
        value = value[1:]
    return value

def extract_pnl_values(text):
    """Извлекает значения из текста PnL с сохранением форматирования"""
    logger.info(f"Raw PnL text: {text}")
    values = ['N/A'] * 7  # [txs1, txs2, total_pnl, pnl_percent, unrealized, duration, total_cost]

    try:
        # Разбиваем текст на строки и удаляем пустые
        lines = [line.strip() for line in text.split('\n') if line.strip()]
        logger.info(f"Split lines: {lines}")

        # Получаем числа TXs
        for i, line in enumerate(lines):
            if '7D TXs' in line:
                tx_values = []
                j = i + 1
                while j < len(lines) and len(tx_values) < 2:
                    current_line = lines[j].strip()
                    if current_line != '/':  # Пропускаем разделитель
                        # Сохраняем числа как есть, включая запятые
                        if re.match(r'^\d+(?:,\d+)*$', current_line):
                            tx_values.append(current_line)
                    j += 1
                if len(tx_values) >= 2:
                    values[0] = tx_values[0]
                    values[1] = tx_values[1]
                break

        # Получаем Total PnL и процент
        for i, line in enumerate(lines):
            if 'Total PnL' in line and i + 1 < len(lines):
                pnl_line = lines[i + 1]
                # Ищем сумму
                amount_match = re.search(r'[\+\-]?\$?([\d,.]+[KMB]?)', pnl_line)
                if amount_match:
                    pnl_value = amount_match.group(1)
                    if '-' in pnl_line and pnl_line.index('-') < pnl_line.index(pnl_value):
                        values[2] = f"-{pnl_value}"
                    else:
                        values[2] = pnl_value

                # Ищем процент
                percent_match = re.search(r'\(([-\+]?\d+\.?\d*)%\)', pnl_line)
                if percent_match:
                    percent_value = percent_match.group(1)
                    if percent_value.startswith('+'):
                        percent_value = percent_value[1:]
                    values[3] = f"{percent_value}%"

        # Словарь соответствия меток и индексов
        label_mapping = {
            'Unrealized Profits': 4,
            '7D Avg Duration': 5,
            '7D Total Cost': 6
        }

        # Получаем остальные значения
        for i, line in enumerate(lines):
            for label, index in label_mapping.items():
                if label in line and i + 1 < len(lines):
                    next_line = lines[i + 1]
                    value = extract_value(next_line)
                    if next_line.startswith('-'):
                        if value.startswith('-'):  # Избегаем двойных минусов
                            values[index] = value
                        else:
                            values[index] = f"-{value}"
                    else:
                        values[index] = value

        logger.info(f"Extracted values: {values}")
        return values

    except Exception as e:
        logger.error(f"Error parsing PnL block: {e}")
        return values

async def setup_browser():
    logger.info("Setting up browser")
    playwright = await async_playwright().start()
    browser = await playwright.chromium.launch(
        headless=True,
        args=[
            '--no-sandbox',
            '--disable-setuid-sandbox',
            '--disable-web-security',
            '--disable-features=IsolateOrigins,site-per-process'
        ]
    )
    return browser, playwright

async def parse_data(url, browser, error_attempt=1):
    logger.info(f"Parsing URL: {url}")
    context_args = {"user_agent": random.choice(USER_AGENTS)}
    if PROXIES:
        context_args["proxy"] = {"server": random.choice(PROXIES)}

    context = await browser.new_context(**context_args)
    page = await context.new_page()

    try:
        await page.goto(url, wait_until="networkidle")
        await asyncio.sleep(CONFIG["PAGE_LOAD_DELAY"])

        results = {
            'col_d': ["N/A"],
            'col_e': ["N/A"],
            'col_f': ["N/A"],
            'pnl_values': ['N/A'] * 7
        }

        # Парсим базовые колонки
        for col in ['col_d', 'col_e', 'col_f']:
            for selector in CONFIG["TARGET_CLASSES"][col]:
                try:
                    element = await page.wait_for_selector(f'.{selector}', timeout=10000)
                    if element:
                        text = await element.inner_text()
                        if text.startswith('+'):
                            text = text[1:]
                        results[col] = [text]
                        logger.info(f"Found {col}: {text}")
                        break
                except Exception as e:
                    logger.error(f"Error parsing {col}: {e}")

        # Парсим PnL блок
        try:
            pnl_element = await page.wait_for_selector('.css-1ug9me3', timeout=10000)
            if pnl_element:
                pnl_text = await pnl_element.inner_text()
                if pnl_text:
                    pnl_values = extract_pnl_values(pnl_text)
                    if pnl_values:
                        results['pnl_values'] = pnl_values
                    else:
                        logger.warning("Failed to extract PnL values")
                        return None
        except Exception as e:
            logger.error(f"Error parsing PnL block: {e}")
            return None

        return results

    except Exception as e:
        logger.error(f"Error in parse_data: {e}")
        if error_attempt < CONFIG["MAX_RETRIES"]:
            await asyncio.sleep(CONFIG["REQUEST_DELAY"] * error_attempt)
            return await parse_data(url, browser, error_attempt + 1)
        return None
    finally:
        await context.close()

async def process_single_url(url, browser):
    for attempt in range(CONFIG["MAX_NA_RETRIES"]):
        result = await parse_data(url, browser)
        if result and any(v != 'N/A' for v in result['pnl_values'][:4]):
            return result
        logger.info(f"Attempt {attempt + 1} failed, retrying after delay...")
        await asyncio.sleep(CONFIG["REQUEST_DELAY"])

    return {
        'col_d': ["N/A"],
        'col_e': ["N/A"],
        'col_f': ["N/A"],
        'pnl_values': ['N/A'] * 7
    }

async def process_urls(urls, browser):
    logger.info(f"Processing {len(urls)} URLs")
    results = []
    for url in urls:
        if url:  # Проверяем, что URL не пустой
            result = await process_single_url(url, browser)
            results.append(result)
            await asyncio.sleep(CONFIG["REQUEST_DELAY"])

    values = []
    for res in results:
        if res:
            # Не преобразуем значения, оставляем их как есть
            row_values = [
                ', '.join(clean_numeric_values(res.get('col_d', [])[:3])),
                ', '.join(clean_numeric_values(res.get('col_e', [])[:3])),
                ', '.join(clean_numeric_values(res.get('col_f', [])[:3])),
                *(res.get('pnl_values', ['N/A'] * 7))
            ]
            logger.info(f"Row values: {row_values}")
            values.append(row_values)

    return values

async def main():
    logger.info("Starting parser")
    try:
        encoded_creds = os.getenv('GOOGLE_CREDENTIALS_BASE64')
        if not encoded_creds:
            raise ValueError("GOOGLE_CREDENTIALS_BASE64 not set")

        with open(CONFIG["CREDS_FILE"], 'w') as f:
            f.write(base64.b64decode(encoded_creds).decode('utf-8'))

        gc = gspread.authorize(
            ServiceAccountCredentials.from_json_keyfile_name(
                CONFIG["CREDS_FILE"],
                ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
            )
        )
        sheet = gc.open_by_key(CONFIG["SPREADSHEET_ID"]).worksheet(CONFIG["SHEET_NAME"])
        logger.info("Connected to Google Sheet")

        browser, playwright = await setup_browser()
        logger.info("Browser setup complete")

        for i in range(0, CONFIG["TOTAL_URLS"], CONFIG["MAX_CONCURRENT_PAGES"]):
            start = CONFIG["START_ROW"] + i
            urls = [sheet.cell(start + j, 3).value for j in range(CONFIG["MAX_CONCURRENT_PAGES"])]
            urls = [url for url in urls if url and url.startswith('http')]
            
            if not urls:
                logger.info(f"No URLs found starting at row {start}")
                continue

            logger.info(f"Processing batch starting at row {start}")
            values = await process_urls(urls, browser)

            if values:
                range_name = f'D{start}:M{start + len(values) - 1}'
                logger.info(f"Updating range {range_name}")
                # Используем 'RAW' вместо 'USER_ENTERED' для сохранения форматирования
                sheet.update(
                    range_name=range_name,
                    values=values,
                    value_input_option='RAW'
                )
                logger.info(f"Updated {len(values)} rows")

            await asyncio.sleep(CONFIG["REQUEST_DELAY"])

        await browser.close()
        await playwright.stop()
        logger.info("Parser finished successfully")

    except Exception as e:
        logger.critical(f"Critical error: {str(e)}", exc_info=True)
    finally:
        if os.path.exists(CONFIG["CREDS_FILE"]):
            os.remove(CONFIG["CREDS_FILE"])

if __name__ == "__main__":
    asyncio.run(main())
