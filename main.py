import asyncio
import base64
import gspread
import logging
import os
import random
import re
from playwright.async_api import async_playwright
from oauth2client.service_account import ServiceAccountCredentials

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
        'pnl_data': ['css-1ug9me3']
    }
}

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
]

PROXIES = []

def extract_pnl_data(text):
    """Извлекает данные из текста PnL"""
    # Удаляем все переносы строк и лишние пробелы
    text = ' '.join(text.split())
    
    # Создаем словарь для хранения извлеченных данных
    data = {
        '7d_txs': 'N/A',
        'total_pnl': 'N/A',
        'unrealized_profits': 'N/A',
        'avg_duration': 'N/A',
        'total_cost': 'N/A',
        'token_avg_cost': 'N/A',
        'realized_profits': 'N/A'
    }
    
    try:
        # Извлекаем 7D TXs (берем первые числа до слеша)
        txs_match = re.search(r'7DTXs\s*(\d+)\s*/\s*(\d+)', text)
        if txs_match:
            data['7d_txs'] = f"{txs_match.group(1)}/{txs_match.group(2)}"

        # Извлекаем Total PnL
        pnl_match = re.search(r'TotalPnL\s*([-\d.KM]+)\s*\(([-\d.]+%)\)', text)
        if pnl_match:
            data['total_pnl'] = f"{pnl_match.group(1)} ({pnl_match.group(2)})"

        # Извлекаем Unrealized Profits
        unr_match = re.search(r'UnrealizedProfits\s*([-\d.KM]+)', text)
        if unr_match:
            data['unrealized_profits'] = unr_match.group(1)

        # Извлекаем 7D Avg Duration
        dur_match = re.search(r'7DAvgDuration\s*(\d+[hd])', text)
        if dur_match:
            data['avg_duration'] = dur_match.group(1)

        # Извлекаем 7D Total Cost
        cost_match = re.search(r'7DTotalCost\s*([-\d.KM]+)', text)
        if cost_match:
            data['total_cost'] = cost_match.group(1)

        # Извлекаем 7D Token Avg Cost
        token_cost_match = re.search(r'7DTokenAvgCost\s*([-\d.,]+)', text)
        if token_cost_match:
            data['token_avg_cost'] = token_cost_match.group(1)

        # Извлекаем 7D Token Avg Realized Profits
        realized_match = re.search(r'7DTokenAvgRealizedProfits\s*([-\d.,]+)', text)
        if realized_match:
            data['realized_profits'] = realized_match.group(1)

    except Exception as e:
        logging.error(f"Error parsing PnL data: {e}")

    return data

def clean_numeric_values(data_list):
    return [item.strip().replace('+', '').replace(' ', '').replace('$', '').replace('€', '').replace('£', '') for item in data_list]

def is_valid_result(result):
    error_markers = {"N/A", "--%", "0%", "0"}
    for col in ['col_d', 'col_e', 'col_f']:  # Проверяем только основные колонки
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
            'pnl_data': {}
        }

        # Парсим основные колонки
        for col in ['col_d', 'col_e', 'col_f']:
            for selector in CONFIG["TARGET_CLASSES"][col]:
                try:
                    await page.wait_for_selector(f'.{selector}', timeout=5000)
                    elements = await page.query_selector_all(f'.{selector}')
                    if elements:
                        results[col] = [await el.inner_text() for el in elements]
                        break
                except Exception:
                    continue

        # Парсим PnL данные
        try:
            pnl_element = await page.query_selector('.css-1ug9me3')
            if pnl_element:
                pnl_text = await pnl_element.inner_text()
                results['pnl_data'] = extract_pnl_data(pnl_text)
        except Exception as e:
            logging.error(f"Error parsing PnL section: {e}")

        return results
    except Exception:
        if error_attempt < CONFIG["MAX_RETRIES"]:
            await asyncio.sleep(CONFIG["REQUEST_DELAY"] * error_attempt)
            return await parse_data(url, browser, error_attempt + 1)
        else:
            return {
                'col_d': ["FAIL"],
                'col_e': ["FAIL"],
                'col_f': ["FAIL"],
                'pnl_data': {}
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
        pnl_data = res.get('pnl_data', {})
        row_values = [
            ', '.join(clean_numeric_values(res.get('col_d', [])[:3])),
            ', '.join(clean_numeric_values(res.get('col_e', [])[:3])),
            ', '.join(clean_numeric_values(res.get('col_f', [])[:3])),
            pnl_data.get('7d_txs', 'N/A'),
            pnl_data.get('total_pnl', 'N/A'),
            pnl_data.get('unrealized_profits', 'N/A'),
            pnl_data.get('avg_duration', 'N/A'),
            pnl_data.get('total_cost', 'N/A'),
            pnl_data.get('token_avg_cost', 'N/A'),
            pnl_data.get('realized_profits', 'N/A')
        ]
        values.append(row_values)
    
    return values

async def main():
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

        browser, playwright = await setup_browser()

        for i in range(0, CONFIG["TOTAL_URLS"], CONFIG["MAX_CONCURRENT_PAGES"]):
            start = CONFIG["START_ROW"] + i
            urls = [sheet.cell(start + j, 3).value for j in range(CONFIG["MAX_CONCURRENT_PAGES"])]
            urls = [url for url in urls if url and url.startswith('http')]
            if not urls:
                continue

            values = await process_urls(urls, browser)

            sheet.update(
                range_name=f'D{start}:M{start + len(values) - 1}', 
                values=values, 
                value_input_option='USER_ENTERED'
            )

            await asyncio.sleep(random.uniform(3, 7))

        await browser.close()
        await playwright.stop()
    except Exception as e:
        logging.critical(f"Critical error: {str(e)}")
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
