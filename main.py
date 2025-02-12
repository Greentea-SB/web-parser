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
        'pnl_block': 'css-1ug9me3'
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

def parse_pnl_block(text):
    """
    Извлекает только числовые значения из PnL блока
    """
    logging.info(f"Raw PnL text: {text}")  # Добавляем логирование
    
    # Очищаем текст от лишних пробелов и переносов
    text = ' '.join(text.split())
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
        # Разбиваем текст на строки
        lines = text.split('\n')
        logging.info(f"Split lines: {lines}")  # Логируем разбитые строки

        # Ищем числа после "7DTXs"
        if "7DTXs" in text:
            numbers = re.findall(r'\d+', text.split("7DTXs")[1].split("TotalPnL")[0])
            if len(numbers) >= 2:
                values['g'] = numbers[0]
                values['h'] = numbers[1]

        # Ищем TotalPnL
        if "TotalPnL" in text:
            pnl_text = text.split("TotalPnL")[1].split("UnrealizedProfits")[0]
            pnl_match = re.search(r'([\d.]+K?M?)\s*\(([-\d.]+)%\)', pnl_text)
            if pnl_match:
                values['i'] = pnl_match.group(1)
                values['j'] = pnl_match.group(2)

        # Ищем UnrealizedProfits
        if "UnrealizedProfits" in text:
            unr_text = text.split("UnrealizedProfits")[1].split("7DAvgDuration")[0]
            unr_match = re.search(r'([\d.]+K?M?)', unr_text)
            if unr_match:
                values['k'] = unr_match.group(1)

        # Ищем 7DTotalCost
        if "7DTotalCost" in text:
            cost_text = text.split("7DTotalCost")[1].split("7DTokenAvgCost")[0]
            cost_match = re.search(r'([\d.]+K?M?)', cost_text)
            if cost_match:
                values['l'] = cost_match.group(1)

        # Ищем последнее число (RealizedProfits)
        if "7DTokenAvgRealizedProfits" in text:
            profit_text = text.split("7DTokenAvgRealizedProfits")[1]
            profit_match = re.search(r'([-\d,.]+)', profit_text)
            if profit_match:
                values['m'] = profit_match.group(1)

        logging.info(f"Extracted values: {values}")  # Логируем извлеченные значения
        return values

    except Exception as e:
        logging.error(f"Error parsing PnL block: {e}")
        return values

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
                        break
                except Exception:
                    continue

        # Парсим PnL блок
        try:
            # Добавляем ожидание элемента
            await page.wait_for_selector('.css-1ug9me3', timeout=5000)
            pnl_elements = await page.query_selector_all('.css-1ug9me3')
            
            if pnl_elements:
                # Получаем текст из всех найденных элементов
                pnl_texts = [await el.inner_text() for el in pnl_elements]
                logging.info(f"Found {len(pnl_elements)} PnL elements")
                logging.info(f"PnL texts: {pnl_texts}")  # Логируем найденные тексты
                
                # Используем первый найденный элемент
                pnl_text = pnl_texts[0] if pnl_texts else ""
                results['pnl_values'] = parse_pnl_block(pnl_text)
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
