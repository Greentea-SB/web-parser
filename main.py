import asyncio
import base64
import gspread
import logging
import os
import random
import re
import time
from playwright.async_api import async_playwright, TimeoutError
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
    "REQUEST_DELAY": 1,
    "PAGE_LOAD_DELAY": 1,
    "BATCH_SIZE": 5,
    "START_ROW": 14,
    "TOTAL_URLS": 260,
    "TARGET_CLASSES": {
        'col_d': ['css-16udrhy', 'css-16udrhy', 'css-nd24it'],
        'col_e': ['css-sahmrr', 'css-kavdos', 'css-1598eja'],
        'col_f': ['css-j4xe5q', 'css-d865bw', 'css-krr03m']
    },
    "NAVIGATION_TIMEOUT": 60000,
    "WAIT_TIMEOUT": 10000
}

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
]

def is_valid_number(text):
    text = text.strip()
    pattern = r'^-?\d+(?:,\d+)*(?:\.\d+)?$'
    return bool(re.match(pattern, text))

def clean_numeric_values(data_list):
    cleaned = []
    for item in data_list:
        if isinstance(item, str):
            item = item.strip()
            if item.startswith('+'):
                item = item[1:]
        cleaned.append(item)
    return cleaned

def extract_value(text):
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
    logger.info(f"Raw PnL text: {text}")
    values = ['N/A'] * 7

    try:
        lines = [line.strip() for line in text.split('\n') if line.strip()]
        logger.info(f"Split lines: {lines}")

        # Получаем числа TXs
        for i, line in enumerate(lines):
            if '7D TXs' in line:
                tx_values = []
                j = i + 1
                while j < len(lines) and len(tx_values) < 2:
                    current_line = lines[j].strip()
                    if current_line != '/':
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
                amount_match = re.search(r'[\+\-]?\$?([\d,.]+[KMB]?)', pnl_line)
                if amount_match:
                    pnl_value = amount_match.group(1)
                    if '-' in pnl_line and pnl_line.index('-') < pnl_line.index(pnl_value):
                        values[2] = f"-{pnl_value}"
                    else:
                        values[2] = pnl_value

                percent_match = re.search(r'\(([-\+]?\d+\.?\d*)%\)', pnl_line)
                if percent_match:
                    percent_value = percent_match.group(1)
                    if percent_value.startswith('+'):
                        percent_value = percent_value[1:]
                    values[3] = f"{percent_value}%"

        label_mapping = {
            'Unrealized Profits': 4,
            '7D Avg Duration': 5,
            '7D Total Cost': 6
        }

        for i, line in enumerate(lines):
            for label, index in label_mapping.items():
                if label in line and i + 1 < len(lines):
                    next_line = lines[i + 1]
                    value = extract_value(next_line)
                    if next_line.startswith('-'):
                        if value.startswith('-'):
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

async def setup_browser(playwright):
    browser = await playwright.chromium.launch(
        headless=True,
        args=[
            '--no-sandbox',
            '--disable-setuid-sandbox',
            '--disable-web-security',
            '--disable-features=IsolateOrigins,site-per-process',
            '--disable-dev-shm-usage',
            '--disable-gpu',
            '--no-first-run',
            '--no-zygote',
            '--single-process'
        ]
    )
    return browser

async def parse_url(url, browser):
    if not url or not url.startswith('http'):
        return None

    context = await browser.new_context(
        user_agent=random.choice(USER_AGENTS),
        viewport={'width': 1920, 'height': 1080}
    )
    page = await context.new_page()
    
    try:
        page.set_default_navigation_timeout(CONFIG["NAVIGATION_TIMEOUT"])
        page.set_default_timeout(CONFIG["WAIT_TIMEOUT"])

        await page.goto(url, wait_until="domcontentloaded")
        await asyncio.sleep(CONFIG["PAGE_LOAD_DELAY"])

        results = {
            'col_d': ["N/A"],
            'col_e': ["N/A"],
            'col_f': ["N/A"],
            'pnl_values': ['N/A'] * 7
        }

        for col in ['col_d', 'col_e', 'col_f']:
            for selector in CONFIG["TARGET_CLASSES"][col]:
                try:
                    element = await page.wait_for_selector(f'.{selector}', timeout=CONFIG["WAIT_TIMEOUT"])
                    if element:
                        text = await element.inner_text()
                        if text.startswith('+'):
                            text = text[1:]
                        results[col] = [text]
                        break
                except Exception:
                    continue

        try:
            pnl_element = await page.wait_for_selector('.css-1ug9me3', timeout=CONFIG["WAIT_TIMEOUT"])
            if pnl_element:
                pnl_text = await pnl_element.inner_text()
                if pnl_text:
                    results['pnl_values'] = extract_pnl_values(pnl_text)
        except Exception as e:
            logger.error(f"Error parsing PnL block for {url}: {e}")

        return results

    except TimeoutError:
        logger.error(f"Timeout error for {url}")
        return None
    except Exception as e:
        logger.error(f"Error processing {url}: {e}")
        return None
    finally:
        await context.close()

async def process_batch(urls, browser):
    tasks = []
    for url in urls:
        if url and url.startswith('http'):
            tasks.append(parse_url(url, browser))
            await asyncio.sleep(0.5)

    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    values = []
    for result in results:
        if result and not isinstance(result, Exception):
            row_values = [
                ', '.join(clean_numeric_values(result.get('col_d', [])[:3])),
                ', '.join(clean_numeric_values(result.get('col_e', [])[:3])),
                ', '.join(clean_numeric_values(result.get('col_f', [])[:3])),
                *(result.get('pnl_values', ['N/A'] * 7))
            ]
            values.append(row_values)
        else:
            values.append(['N/A'] * 10)
    
    return values

async def main():
    logger.info("Starting parser")
    try:
        # Получаем учетные данные
        encoded_creds = os.getenv('GOOGLE_CREDENTIALS_BASE64')
        if not encoded_creds:
            raise ValueError("GOOGLE_CREDENTIALS_BASE64 not set")

        with open(CONFIG["CREDS_FILE"], 'w') as f:
            f.write(base64.b64decode(encoded_creds).decode('utf-8'))

        # Инициализация Google Sheets
        gc = gspread.authorize(
            ServiceAccountCredentials.from_json_keyfile_name(
                CONFIG["CREDS_FILE"],
                ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
            )
        )
        sheet = gc.open_by_key(CONFIG["SPREADSHEET_ID"]).worksheet(CONFIG["SHEET_NAME"])
        logger.info("Connected to Google Sheet")

        # Получаем все URL сразу
        end_row = CONFIG["START_ROW"] + CONFIG["TOTAL_URLS"]
        range_name = f'C{CONFIG["START_ROW"]}:C{end_row}'
        all_urls = sheet.get(range_name)
        all_urls = [url[0] if url else None for url in all_urls]
        logger.info(f"Retrieved {len(all_urls)} URLs")

        async with async_playwright() as playwright:
            browser = await setup_browser(playwright)
            
            # Обрабатываем URL батчами
            results = []
            for i in range(0, len(all_urls), CONFIG["BATCH_SIZE"]):
                batch_urls = all_urls[i:i + CONFIG["BATCH_SIZE"]]
                start_row = CONFIG["START_ROW"] + i
                
                logger.info(f"Processing batch of {len(batch_urls)} URLs starting at row {start_row}")
                batch_results = await process_batch(batch_urls, browser)
                
                if batch_results:
                    # Обновляем результаты в таблице
                    range_name = f'D{start_row}:M{start_row + len(batch_results) - 1}'
                    logger.info(f"Updating range {range_name}")
                    
                    try:
                        sheet.update(
                            range_name=range_name,
                            values=batch_results,
                            value_input_option='RAW'
                        )
                        logger.info(f"Updated {len(batch_results)} rows")
                    except Exception as e:
                        logger.error(f"Error updating sheet: {e}")
                        # Добавляем задержку при ошибке
                        await asyncio.sleep(60)
                        # Пытаемся обновить снова
                        sheet.update(
                            range_name=range_name,
                            values=batch_results,
                            value_input_option='RAW'
                        )
                
                # Добавляем задержку между батчами
                await asyncio.sleep(3)

            await browser.close()

    except Exception as e:
        logger.critical(f"Critical error: {str(e)}", exc_info=True)
    finally:
        if os.path.exists(CONFIG["CREDS_FILE"]):
            os.remove(CONFIG["CREDS_FILE"])

if __name__ == "__main__":
    asyncio.run(main())
