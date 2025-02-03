def parse_data(url, browser):
    """Парсинг данных с повторными попытками"""
    for attempt in range(MAX_RETRIES):
        try:
            logging.info(f"Парсинг URL: {url} (попытка {attempt + 1})")
            page = browser.new_page()
            page.goto(url, timeout=60000)
            
            # Увеличиваем таймаут и проверяем наличие элемента
            page.wait_for_selector(f'.{TARGET_CLASSES["col_d"]}', timeout=30000, state="attached")
            
            # Проверяем существование элементов
            d_element = page.query_selector(f'.{TARGET_CLASSES["col_d"]}')
            e_element = page.query_selector(f'.{TARGET_CLASSES["col_e"]}')
            f_element = page.query_selector(f'.{TARGET_CLASSES["col_f"]}')
            
            if not all([d_element, e_element, f_element]):
                logging.error("Не все элементы найдены")
                return {'d': 'N/A', 'e': 'N/A', 'f': 'N/A'}
            
            result = {
                'd': d_element.inner_text().strip(),
                'e': e_element.inner_text().strip(),
                'f': f_element.inner_text().strip()
            }
            
            page.close()
            return result
            
        except Exception as e:
            logging.error(f"Ошибка при парсинге {url}: {str(e)}")
            time.sleep(REQUEST_DELAY)
        finally:
            if 'page' in locals() and not page.is_closed():
                page.close()

def update_sheet(sheet, row, data):
    """Обновление строки в таблице"""
    try:
        sheet.update(
            range_name=f'D{row}:G{row}',  # Расширенный диапазон
            values=[[data['d'], data['e'], data['f'], datetime.now().strftime("%Y-%m-%d %H:%M:%S")]],
            value_input_option='USER_ENTERED'
        )
    except Exception as e:
        logging.error(f"Ошибка при обновлении таблицы: {str(e)}")
