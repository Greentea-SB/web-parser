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
    "REQUEST_DELAY": 5,
    "MAX_CONCURRENT_PAGES": 10,
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

def extract_pnl_values(text):
    """Извлекает значения из текста PnL с сохранением форматирования"""
    logger.info(f"Raw PnL text: {text}")
    values = ['N/A'] * 7
    
    try:
        lines = text.strip().split('\n')
        lines = [line.strip() for line in lines if line.strip()]
        logger.info(f"Split lines: {lines}")

        current_section = None
        number_values = []

        for line in lines:
            if 'TXs' in line:
                current_section = 'txs'
                continue
            elif 'TotalPnL' in line:
                current_section = 'pnl'
                continue
            elif 'UnrealizedProfits' in line:
                current_section = 'unrealized'
                continue
            elif 'Duration' in line:
                current_section = 'duration'
                continue
            elif 'TotalCost' in line:
                current_section = 'cost'
                continue
            elif 'RealizedProfits' in line:
                current_section = 'realized'
                continue

            if current_section == 'txs' and line.isdigit():
                number_values.append(line)
                if len(number_values) <= 2:
                    values[len(number_values)-1] = line

            elif current_section == 'pnl':
                pnl_match = re.match(r'[\+\-]?\$?([\d,.]+K?M?)\s*\(([-\+]?[\d.]+)%\)', line)
                if pnl_match:
                    values[2] = pnl_match.group(1)
                    values[3] = f"{pnl_match.group(2)}%"

            elif current_section == 'unrealized':
                unr_match = re.match(r'[\+\-]?\$?([\d,.]+K?M?)', line)
                if unr_match:
                    values[4] = unr_match.group(1)

            elif current_section == 'duration':
                dur_match = re.match(r'([\d.]+[dhm])', line)
                if dur_match:
                    values[5] = dur_match.group(1)

            elif current_section == 'cost':
                cost_match = re.match(r'[\+\-]?\$?([\d,.]+K?M?)', line)
                if cost_match:
                    values[6] = cost_match.group(1)

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
        response = await page.goto(url, wait_until="domcontentloaded")
        if not response:
            logger.error(f"Failed to load page: {url}")
            return None
        
        await asyncio.sleep(random.uniform(1.0, 2.5))

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
                    element = await page.wait_for_selector(f'.{selector}', timeout=5000)
                    if element:
                        text = await element.inner_text()
                        results[col] = [text]
                        logger.info(f"Found {col}: {text}")
                        break
                except Exception as e:
                    logger.error(f"Error parsing {col}: {e}")

        # Парсим PnL блок
        try:
            pnl_element = await page.wait_for_selector('.css-1ug9me3', timeout=5000)
            if pnl_element:
                pnl_text = await pnl_element.inner_text()
                if pnl_text:
                    results['pnl_values'] = extract_pnl_values(pnl_text)
                    logger.info(f"PnL values: {results['pnl_values']}")
        except Exception as e:
            logger.error(f"Error parsing PnL block: {e}")

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
    result = await parse_data(url, browser)
    if not result:
        logger.error(f"Failed to process URL: {url}")
        return {
            'col_d': ["N/A"],
            'col_e': ["N/A"],
            'col_f': ["N/A"],
            'pnl_values': ['N/A'] * 7
        }
    return result

async def process_urls(urls, browser):
    logger.info(f"Processing {len(urls)} URLs")
    tasks = [process_single_url(url, browser) for url in urls]
    results_list = await asyncio.gather(*tasks)
    
    values = []
    for res in results_list:
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

        # Сохраняем credentials
        with open(CONFIG["CREDS_FILE"], 'w') as f:
            f.write(base64.b64decode(encoded_creds).decode('utf-8'))

        # Подключаемся к Google Sheets
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

            await asyncio.sleep(random.uniform(3, 7))

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
