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
    "MAX_CONCURRENT_PAGES": 10,
    "START_ROW": 14,
    "TOTAL_URLS": 260,
    "TARGET_CLASS": 'css-j7qwjs'  # Целевой класс для парсинга
}

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
]

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
    context = await browser.new_context(
        user_agent=random.choice(USER_AGENTS)
    )
    page = await context.new_page()

    try:
        await page.goto(url, wait_until="domcontentloaded")
        await asyncio.sleep(random.uniform(1.0, 2.5))

        # Ждем появления целевого элемента
        await page.wait_for_selector(f'.{CONFIG["TARGET_CLASS"]}', timeout=5000)
        
        # Получаем весь HTML-контент элемента
        content = await page.evaluate(f'''
            () => {{
                const element = document.querySelector('.{CONFIG["TARGET_CLASS"]}');
                return element ? element.innerText : null;
            }}
        ''')
        
        # Получаем все вложенные элементы
        elements = await page.query_selector_all(f'.{CONFIG["TARGET_CLASS"]} *')
        nested_content = []
        for element in elements:
            try:
                text = await element.inner_text()
                if text.strip():
                    nested_content.append(text.strip())
            except:
                continue

        return {
            'main_content': content,
            'nested_content': nested_content
        }

    except Exception as e:
        if error_attempt < CONFIG["MAX_RETRIES"]:
            await asyncio.sleep(CONFIG["REQUEST_DELAY"] * error_attempt)
            return await parse_data(url, browser, error_attempt + 1)
        else:
            return {'main_content': 'FAIL', 'nested_content': []}
    finally:
        await context.close()

async def process_single_url(url, browser):
    result = await parse_data(url, browser)
    return result

async def process_urls(urls, browser):
    tasks = [process_single_url(url, browser) for url in urls]
    return await asyncio.gather(*tasks)

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

            results_list = await process_urls(urls, browser)

            values = []
            for result in results_list:
                # Форматируем данные для записи в таблицу
                main_content = result['main_content'] if result['main_content'] != 'FAIL' else 'FAIL'
                nested_content = ' | '.join(result['nested_content']) if result['nested_content'] else 'No content'
                values.append([main_content, nested_content])

            # Записываем результаты в таблицу (колонки D и E)
            sheet.update(
                range_name=f'D{start}:E{start + len(values) - 1}', 
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
