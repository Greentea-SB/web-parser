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
        'col_f': ['css-j4xe5q', 'css-d865bw', 'css-krr03m'],
        'pnl_block': 'css-1ug9me3'
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
    return [item.strip().replace('+', '').replace(' ', '').replace('$', '').replace('€', '').replace('£', '') for item in data_list]

def parse_pnl_block(text):
    """Извлекает числовые значения из PnL блока"""
    logging.info(f"Starting to parse PnL block. Raw text: {text}")
    
    values = {
        'g': 'N/A',
        'h': 'N/A',
        'i': 'N/A',
        'j': 'N/A',
        'k': 'N/A',
        'l': 'N/A',
        'm': 'N/A'
    }
    
    try:
        # Разбиваем на строки и очищаем
        lines = [line.strip() for line in text.split('\n') if line.strip()]
        logging.info(f"Split lines: {lines}")

        # Поиск TXs чисел (первые два числа после 7DTXs)
        for i, line in enumerate(lines):
            if '7DTXs' in line:
                # Ищем следующие две строки с числами
                numbers = []
                for j in range(i+1, min(i+5, len(lines))):
                    if lines[j].isdigit():
                        numbers.append(lines[j])
                    if len(numbers) == 2:
                        break
                if len(numbers) >= 2:
                    values['g'] = numbers[0]
                    values['h'] = numbers[1]
                break

        # Поиск TotalPnL
        for i, line in enumerate(lines):
            if 'TotalPnL' in line:
                next_line = lines[i+1] if i+1 < len(lines) else ''
                pnl_match = re.search(r'([\d.]+K?M?)\s*\(([-\d.]+)%\)', next_line)
                if pnl_match:
                    values['i'] = pnl_match.group(1)
                    values['j'] = pnl_match.group(2)
                break

        # Поиск UnrealizedProfits
        for i, line in enumerate(lines):
            if 'UnrealizedProfits' in line:
                next_line = lines[i+1] if i+1 < len(lines) else ''
                if next_line:
                    values['k'] = re.search(r'([\d.]+K?M?)', next_line).group(1)
                break

        # Поиск 7DTotalCost
        for i, line in enumerate(lines):
            if '7DTotalCost' in line:
                next_line = lines[i+1] if i+1 < len(lines) else ''
                if next_line:
                    values['l'] = re.search(r'([\d.]+K?M?)', next_line).group(1)
                break

        # Поиск RealizedProfits (последнее число)
        for i, line in enumerate(lines):
            if '7DTokenAvgRealizedProfits' in line:
                next_line = lines[i+1] if i+1 < len(lines) else ''
                if next_line:
                    values['m'] = re.search(r'([-\d,.]+)', next_line).group(1)
                break

        logging.info(f"Extracted values: {values}")
        return values

    except Exception as e:
        logging.error(f"Error parsing PnL block: {e}")
        return values

def is_valid_result(result):
    error_markers = {"N/A", "--%", "0%", "0"}
    for col in ['col_d', 'col_e', 'col_f']:
        if not result.get(col) or result[col][0] in error_markers:
            return False
    return True

async def setup_browser():
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
    logging.info(f"Starting to parse URL: {url}")
    context_args = {
        "user_agent": random.choice(USER_AGENTS)
    }

    if PROXIES:
        context_args["proxy"] = {"server": random.choice(PROXIES)}

    context = await browser.new_context(**context_args)
    page = await context.new_page()

    try:
        await page.goto(url, wait_until="domcontentloaded")
        await asyncio.sleep(random.uniform(1.0, 2.5))

        results = {
            'col_d': ["N/A"],
            'col_e': ["N/A"],
            'col_f': ["N/A"],
            'pnl_values': {}
        }

        # Парсим базовые колонки
        for col in ['col_d', 'col_e', 'col_f']:
            for selector in CONFIG["TARGET_CLASSES"][col]:
                try:
                    await page.wait_for_selector(f'.{selector}', timeout=5000)
                    elements = await page.query_selector_all(f'.{selector}')
                    if elements:
                        results[col] = [await el.inner_text() for el in elements]
                        logging.info(f"Found {col} values: {results[col]}")
                        break
                except Exception as e:
                    logging.error(f"Error parsing {col}: {e}")

        # Парсим PnL блок
        try:
            await page.wait_for_selector('.css-1ug9me3', timeout=5000)
            pnl_elements = await page.query_selector_all('.css-1ug9me3')
            
            if pnl_elements:
                logging.info(f"Found {len(pnl_elements)} PnL elements")
                for i, el in enumerate(pnl_elements):
                    text = await el.inner_text()
                    logging.info(f"PnL element {i} text: {text}")
                    if '7DTXs' in text:  # Берем только нужный блок
                        results['pnl_values'] = parse_pnl_block(text)
                        break
            else:
                logging.warning("No PnL elements found")
                results['pnl_values'] = {k: 'N/A' for k in 'ghijklm'}
        except Exception as e:
            logging.error(f"Error getting PnL block: {e}")
            results['pnl_values'] = {k: 'N/A' for k in 'ghijklm'}

        return results

    except Exception as e:
        logging.error(f"Error in parse_data: {e}")
        if error_attempt < CONFIG["MAX_RETRIES"]:
            await asyncio.sleep(CONFIG["REQUEST_DELAY"] * error_attempt)
            return await parse_data(url, browser, error_attempt + 1)
        else:
            return {
                'col_d': ["FAIL"],
                'col_e': ["FAIL"],
                'col_f': ["FAIL"],
                'pnl_values': {k: 'FAIL' for k in 'ghijklm'}
            }
    finally:
        await context.close()

async def process_single_url(url, browser):
    for na_attempt in range(CONFIG["MAX_NA_RETRIES"]):
        result = await parse_data(url, browser)
        if is_valid_result(result):
            return result
        await asyncio.sleep(CONFIG["REQUEST_DELAY"])
    return result

async def process_urls(urls, browser):
    tasks = [process_single_url(url, browser) for url in urls]
    results_list = await asyncio.gather(*tasks)
    
    values = []
    for res in results_list:
        pnl_vals = res.get('pnl_values', {})
        row_values = [
            ', '.join(clean_numeric_values(res.get('col_d', [])[:3])),
            ', '.join(clean_numeric_values(res.get('col_e', [])[:3])),
            ', '.join(clean_numeric_values(res.get('col_f', [])[:3])),
            pnl_vals.get('g', 'N/A'),
            pnl_vals.get('h', 'N/A'),
            pnl_vals.get('i', 'N/A'),
            pnl_vals.get('j', 'N/A'),
            pnl_vals.get('k', 'N/A'),
            pnl_vals.get('l', 'N/A'),
            pnl_vals.get('m', 'N/A')
        ]
        logging.info(f"Prepared row values: {row_values}")
        values.append(row_values)
    
    return values

async def main():
    logging.info("Starting parser")
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
        logging.info("Connected to Google Sheet")

        browser, playwright = await setup_browser()
        logging.info("Browser setup complete")

        for i in range(0, CONFIG["TOTAL_URLS"], CONFIG["MAX_CONCURRENT_PAGES"]):
            start = CONFIG["START_ROW"] + i
            urls = [sheet.cell(start + j, 3).value for j in range(CONFIG["MAX_CONCURRENT_PAGES"])]
            urls = [url for url in urls if url and url.startswith('http')]
            if not urls:
                continue

            logging.info(f"Processing batch of URLs starting at row {start}")
            values = await process_urls(urls, browser)

            logging.info(f"Updating sheet range D{start}:M{start + len(values) - 1}")
            sheet.update(
                range_name=f'D{start}:M{start + len(values) - 1}', 
                values=values, 
                value_input_option='USER_ENTERED'
            )

            await asyncio.sleep(random.uniform(3, 7))

        await browser.close()
        await playwright.stop()
        logging.info("Parser finished successfully")
    except Exception as e:
        logging.critical(f"Critical error: {str(e)}")
    finally:
        if os.path.exists(CONFIG["CREDS_FILE"]):
            os.remove(CONFIG["CREDS_FILE"])

if __name__ == "__main__":
    asyncio.run(main())
