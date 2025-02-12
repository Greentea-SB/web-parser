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
    "TARGET_CLASS": 'css-j7qwjs'
}

async def parse_data(url, browser, error_attempt=1):
    context = await browser.new_context(
        user_agent=random.choice(USER_AGENTS)
    )
    page = await context.new_page()

    try:
        await page.goto(url, wait_until="domcontentloaded")
        await asyncio.sleep(random.uniform(1.0, 2.5))

        await page.wait_for_selector(f'.{CONFIG["TARGET_CLASS"]}', timeout=5000)
        
        content = await page.evaluate(f'''
            () => {{
                const element = document.querySelector('.{CONFIG["TARGET_CLASS"]}');
                if (!element) return null;
                
                const text = element.innerText;
                const lines = text.split('\\n');
                
                // Ищем нужные значения
                let pnl = null;
                let winRate = null;
                let balance = null;
                
                for (const line of lines) {{
                    if (line.includes('Last 7D PnL')) {{
                        pnl = lines[lines.indexOf(line) + 1];
                    }}
                    if (line.includes('Win Rate')) {{
                        winRate = lines[lines.indexOf(line) + 1];
                    }}
                    if (line.includes('USD')) {{
                        balance = lines[lines.indexOf(line) - 1];
                    }}
                }}
                
                return {{
                    pnl: pnl,
                    winRate: winRate,
                    balance: balance
                }};
            }}
        ''')
        
        return content or {'pnl': 'N/A', 'winRate': 'N/A', 'balance': 'N/A'}

    except Exception as e:
        if error_attempt < CONFIG["MAX_RETRIES"]:
            await asyncio.sleep(CONFIG["REQUEST_DELAY"] * error_attempt)
            return await parse_data(url, browser, error_attempt + 1)
        else:
            return {'pnl': 'FAIL', 'winRate': 'FAIL', 'balance': 'FAIL'}
    finally:
        await context.close()

# В функции main изменяем обработку результатов:
            values = []
            for result in results_list:
                values.append([
                    result['pnl'],      # Колонка D
                    result['winRate'],  # Колонка E
                    result['balance']   # Колонка F
                ])

            # Обновляем три колонки
            sheet.update(
                range_name=f'D{start}:F{start + len(values) - 1}', 
                values=values, 
                value_input_option='USER_ENTERED'
            )
