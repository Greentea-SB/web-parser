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
    return [item.strip() for item in data_list]

def extract_pnl_values(text):
    """Извлекает значения из текста PnL с сохранением форматирования"""
    values = []
    
    # Разбиваем текст на строки
    lines = [line.strip() for line in text.split('\n') if line.strip()]
    
    try:
        # Ищем первые два числа (TXs)
        tx_numbers = []
        for line in lines:
            if line.isdigit():
                tx_numbers.append(line)
                if len(tx_numbers) == 2:
                    break
        values.extend(tx_numbers[:2])

        # Ищем Total PnL (два значения - сумма и процент)
        for i, line in enumerate(lines):
            if 'TotalPnL' in line and i + 1 < len(lines):
                pnl_line = lines[i + 1]
                # Извлекаем сумму и процент отдельно
                pnl_match = re.match(r'[\+\-]?\$?([\d,.]+K?M?)\s*\(([-\+]?[\d.]+)%\)', pnl_line)
                if pnl_match:
                    values.append(pnl_match.group(1))  # Сумма с K/M
                    values.append(pnl_match.group(2) + '%')  # Процент

        # Ищем Unrealized Profits
        for i, line in enumerate(lines):
            if 'UnrealizedProfits' in line and i + 1 < len(lines):
                unr_line = lines[i + 1]
                if unr_line.startswith('$'):
                    unr_line = unr_line[1:]  # Убираем первый символ $
                values.append(unr_line)  # Сохраняем полное значение

        # Ищем Duration
        for i, line in enumerate(lines):
            if 'Duration' in line and i + 1 < len(lines):
                dur_line = lines[i + 1]
                values.append(dur_line)  # Сохраняем значение с единицей измерения (d/h/m)

        # Ищем TotalCost
        for i, line in enumerate(lines):
            if 'TotalCost' in line and i + 1 < len(lines):
                cost_line = lines[i + 1]
                if cost_line.startswith('$'):
                    cost_line = cost_line[1:]  # Убираем первый символ $
                values.append(cost_line)

        # Ищем TokenAvgRealizedProfits
        for i, line in enumerate(lines):
            if 'RealizedProfits' in line and i + 1 < len(lines):
                profit_line = lines[i + 1]
                if profit_line.startswith('$'):
                    profit_line = profit_line[1:]  # Убираем первый символ $
                values.append(profit_line)

    except Exception as e:
        logging.error(f"Error extracting PnL values: {e}")
    
    # Если какие-то значения не найдены, заполняем их N/A
    while len(values) < 7:
        values.append('N/A')
    
    logging.info(f"Extracted PnL values: {values}")
    return values[:7]  # Возвращаем только первые 7 значений

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
    context_args = {"user_agent": random.choice(USER_AGENTS)}
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
            'pnl_values': []
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
            pnl_element = await page.query_selector('.css-1ug9me3')
            if pnl_element:
                pnl_text = await pnl_element.inner_text()
                if pnl_text:
                    results['pnl_values'] = extract_pnl_values(pnl_text)
                    logging.info(f"Extracted PnL values: {results['pnl_values']}")
        except Exception as e:
            logging.error(f"Error getting PnL block: {e}")

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
                'pnl_values': ['N/A'] * 7
            }
    finally:
        await context.close()
