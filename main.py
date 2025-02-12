import asyncio
import base64
import gspread
import logging
import os
import random
import re
from playwright.async_api import async_playwright, TimeoutError
from oauth2client.service_account import ServiceAccountCredentials

CONFIG = {
    "SPREADSHEET_ID": "1loVjBMvaO-Ia5JnzMTz8YaGqq10XDz-L1LRWNDDVzsE",
    "SHEET_NAME": "pars",
    "CREDS_FILE": "temp_key.json",
    "MAX_RETRIES": 3,
    "MAX_NA_RETRIES": 5,
    "REQUEST_DELAY": 5,
    "MAX_CONCURRENT_PAGES": 5,  # Уменьшено количество одновременных запросов
    "START_ROW": 14,
    "TOTAL_URLS": 260,
    "TARGET_CLASS": 'css-j7qwjs',
    "PAGE_TIMEOUT": 60000  # Увеличен таймаут до 60 секунд
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
        user_agent=random.choice(USER_AGENTS),
        viewport={'width': 1920, 'height': 1080}
    )
    page = await context.new_page()

    try:
        # Устанавливаем таймаут и пробуем загрузить страницу
        page.set_default_timeout(CONFIG["PAGE_TIMEOUT"])
        
        try:
            # Сначала пробуем с domcontentloaded
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)
        except TimeoutError:
            # Если не получилось, пробуем без ожидания загрузки
            await page.goto(url, wait_until="commit", timeout=30000)

        # Ждем появления целевого элемента
        try:
            await page.wait_for_selector(f'.{CONFIG["TARGET_CLASS"]}', timeout=15000)
        except TimeoutError:
            logging.warning(f"Target element not found for {url}")
            return {'pnl': 'N/A', 'winRate': 'N/A', 'balance': 'N/A'}

        # Даем странице немного времени на загрузку динамического контента
        await asyncio.sleep(random.uniform(2.0, 3.0))

        # Получаем данные
        content = await page.evaluate('''
            () => {
                const element = document.querySelector('.css-j7qwjs');
                if (!element) return null;

                const text = element.innerText;
                const lines = text.split('\\n');

                const findValue = (marker) => {
                    for (let i = 0; i < lines.length; i++) {
                        if (lines[i].includes(marker)) {
                            return lines[i + 1] || null;
                        }
                    }
                    return null;
                };

                return {
                    pnl: findValue('Last 7D PnL'),
                    winRate: findValue('Win Rate'),
                    balance: findValue('USD')
                };
            }
        ''')

        if not content:
            # Пробуем альтернативный метод
            element = await page.query_selector(f'.{CONFIG["TARGET_CLASS"]}')
            if element:
                text = await element.inner_text()
                pnl_match = re.search(r'Last 7D PnL\s*([+\-]?\d+\.?\d*%)', text)
                win_rate_match = re.search(r'Win Rate\s*(\d+\.?\d*%)', text)
                balance_match = re.search(r'([+\-]?\$[\d,]+\.?\d*)\s*USD', text)

                content = {
                    'pnl': pnl_match.group(1) if pnl_match else 'N/A',
                    'winRate': win_rate_match.group(1) if win_rate_match else 'N/A',
                    'balance': balance_match.group(1) if balance_match else 'N/A'
                }

        return content or {'pnl': 'N/A', 'winRate': 'N/A', 'balance': 'N/A'}

    except Exception as e:
        logging.error(f"Error parsing {url}: {str(e)}")
        if error_attempt < CONFIG["MAX_RETRIES"]:
            await asyncio.sleep(CONFIG["REQUEST_DELAY"] * error_attempt)
            return await parse_data(url, browser, error_attempt + 1)
        return {'pnl': 'FAIL', 'winRate': 'FAIL', 'balance': 'FAIL'}
    finally:
        await context.close()

async def process_single_url(url, browser):
    try:
        return await parse_data(url, browser)
    except Exception as e:
        logging.error(f"Error processing {url}: {str(e)}")
        return {'pnl': 'FAIL', 'winRate': 'FAIL', 'balance': 'FAIL'}

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
                values.append([
                    result['pnl'],
                    result['winRate'],
                    result['balance']
                ])

            logging.info(f"Writing values to sheet: {values}")

            sheet.update(
                range_name=f'D{start}:F{start + len(values) - 1}', 
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
