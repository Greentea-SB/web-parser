import asyncio
import base64
import gspread
import logging
import os
import random
from playwright.async_api import async_playwright
from oauth2client.service_account import ServiceAccountCredentials

CONFIG = {
    "SPREADSHEET_ID": "1loVjBMvaO-Ia5JnzMTz8YaGqq10XDz-L1LRWNDDVzsE",
    "SHEET_NAME": "pars",
    "CREDS_FILE": "temp_key.json",
    "MAX_RETRIES": 3,
    "MAX_NA_RETRIES": 5,
    "REQUEST_DELAY": 5,
    "MAX_CONCURRENT_PAGES": 5,
    "START_ROW": 14,
    "TOTAL_URLS": 260,
    "TARGET_CLASSES": {
        'col_d': ['css-16udrhy', 'css-16udrhy', 'css-nd24it'],
        'col_e': ['css-sahmrr', 'css-kavdos', 'css-1598eja'],
        'col_f': ['css-j4xe5q', 'css-d865bw', 'css-krr03m'],
        'col_g': ['css-1ug9me3'],  # 7D TXs
        'col_h': ['css-1ug9me3'],  # Total PnL
        'col_i': ['css-1ug9me3'],  # Unrealized Profits
        'col_j': ['css-1ug9me3'],  # 7D Avg Duration
        'col_k': ['css-1ug9me3'],  # 7D Total Cost
        'col_l': ['css-1ug9me3'],  # 7D Token Avg Cost
        'col_m': ['css-1ug9me3']   # 7D Token Avg Realized Profits
    }
}

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
]

PROXIES = [
    # Пример прокси (замени на свои)
    # "http://username:password@ip:port",
    # "http://ip:port",
]

def clean_numeric_values(data_list):
    return [item.strip().replace('+', '').replace(' ', '').replace('$', '').replace('€', '').replace('£', '') for item in data_list]

def is_valid_result(result):
    error_markers = {"N/A", "--%", "0%", "0"}
    for col in CONFIG["TARGET_CLASSES"]:
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

        results = {col: ["N/A"] for col in CONFIG["TARGET_CLASSES"]}
        
        # Handle original columns (d, e, f)
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

        # Handle PnL data columns (g through m)
        try:
            pnl_elements = await page.query_selector_all('.css-1ug9me3')
            if pnl_elements:
                pnl_texts = [await el.inner_text() for el in pnl_elements]
                pnl_mapping = {
                    'col_g': 0,  # 7D TXs
                    'col_h': 3,  # Total PnL
                    'col_i': 4,  # Unrealized Profits
                    'col_j': 5,  # 7D Avg Duration
                    'col_k': 6,  # 7D Total Cost
                    'col_l': 7,  # 7D Token Avg Cost
                    'col_m': 8,  # 7D Token Avg Realized Profits
                }
                for col, idx in pnl_mapping.items():
                    if idx < len(pnl_texts):
                        results[col] = [pnl_texts[idx]]
        except Exception:
            pass

        return results
    except Exception:
        if error_attempt < CONFIG["MAX_RETRIES"]:
            await asyncio.sleep(CONFIG["REQUEST_DELAY"] * error_attempt)
            return await parse_data(url, browser, error_attempt + 1)
        else:
            return {col: ["FAIL"] for col in CONFIG["TARGET_CLASSES"]}
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
        row_values = [
            ', '.join(clean_numeric_values(res.get('col_d', [])[:3])),
            ', '.join(clean_numeric_values(res.get('col_e', [])[:3])),
            ', '.join(clean_numeric_values(res.get('col_f', [])[:3])),
            ', '.join(clean_numeric_values(res.get('col_g', [])[:1])),
            ', '.join(clean_numeric_values(res.get('col_h', [])[:1])),
            ', '.join(clean_numeric_values(res.get('col_i', [])[:1])),
            ', '.join(clean_numeric_values(res.get('col_j', [])[:1])),
            ', '.join(clean_numeric_values(res.get('col_k', [])[:1])),
            ', '.join(clean_numeric_values(res.get('col_l', [])[:1])),
            ', '.join(clean_numeric_values(res.get('col_m', [])[:1]))
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
