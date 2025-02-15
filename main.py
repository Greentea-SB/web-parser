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
    "START_ROW": 14,
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

def clean_numeric_values(data_list):
    return [item.strip() for item in data_list]

def find_value_after_label(lines, label):
    for i, line in enumerate(lines):
        if label in line and i + 1 < len(lines):
            return lines[i + 1]
    return None

def extract_pnl_values(text):
    """Извлекает значения из текста PnL с сохранением форматирования"""
    logger.info(f"Raw PnL text: {text}")
    values = ['N/A'] * 7  # [txs1, txs2, total_pnl, pnl_percent, unrealized, duration, total_cost]
    
    try:
        # Разбиваем текст на строки и удаляем пустые
        lines = [line.strip() for line in text.split('\n') if line.strip()]
        logger.info(f"Split lines: {lines}")

        # Получаем числа TXs
        current_section = None
        tx_numbers = []
        for line in lines:
            if '7D TXs' in line:
                current_section = 'txs'
                continue
            if current_section == 'txs' and line.isdigit():
                tx_numbers.append(line)
                if len(tx_numbers) == 2:
                    values[0] = tx_numbers[0]
                    values[1] = tx_numbers[1]
                    break

        # Получаем Total PnL и процент
        total_pnl_line = find_value_after_label(lines, 'Total PnL')
        if total_pnl_line:
            # Извлекаем сумму и процент
            match = re.match(r'[\+\-]?\$?([\d,.]+K?M?)\s*\(([-\+]?\d+\.?\d*)%\)', total_pnl_line)
            if match:
                values[2] = match.group(1)  # Сумма
                values[3] = match.group(2) + '%'  # Процент

        # Получаем Unrealized Profits
        unrealized_line = find_value_after_label(lines, 'UnrealizedProfits')
        if unrealized_line:
            # Убираем знак доллара и обрабатываем отрицательные значения
            if unrealized_line.startswith('-$'):
                values[4] = '-' + unrealized_line[2:]
            elif unrealized_line.startswith('$'):
                values[4] = unrealized_line[1:]
            else:
                values[4] = unrealized_line

        # Получаем Duration
        duration_line = find_value_after_label(lines, 'Duration')
        if duration_line:
            values[5] = duration_line

        # Получаем Total Cost
        total_cost_line = find_value_after_label(lines, 'TotalCost')
        if total_cost_line:
            # Убираем знак доллара и обрабатываем отрицательные значения
            if total_cost_line.startswith('-$'):
                values[6] = '-' + total_cost_line[2:]
            elif total_cost_line.startswith('$'):
                values[6] = total_cost_line[1:]
            else:
                values[6] = total_cost_line

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
        result = await process_single_url(url, browser)
        results.append(result)
        await asyncio.sleep(CONFIG["REQUEST_DELAY"])
    
    values = []
    for res in results:
        if res:
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
                sheet.update(
                    range_name=range_name,
                    values=values,
                    value_input_option='USER_ENTERED'
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
