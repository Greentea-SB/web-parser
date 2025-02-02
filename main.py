from playwright.sync_api import sync_playwright
import random
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(message)s",
    handlers=[logging.FileHandler("debug.log"), logging.StreamHandler()]
)

def parse_data():
    try:
        with sync_playwright() as p:
            browser = p.firefox.launch(headless=True)
            page = browser.new_page()
            
            # Добавляем случайный параметр для обхода кэша
            url = f"https://gmgn.ai/sol/address/7XJFKYt7fgABrxXZzkpqS7bUyPwAWaz1WSC2tfGW8nxh?nocache={random.randint(1, 1000)}"
            page.goto(url)
            
            # Увеличиваем таймаут и добавляем проверку
            page.wait_for_selector(".css-16udrhy", timeout=20000)
            page.wait_for_timeout(3000)  # Дополнительное ожидание
            
            target_div = page.query_selector(".css-16udrhy")
            if target_div:
                percentage = target_div.inner_text()
                logging.info(f"Успешно: {percentage}")
                with open("output.txt", "a") as f:
                    f.write(f"{percentage}\n")
            else:
                logging.warning("Элемент не найден.")
            
            browser.close()
    except Exception as e:
        logging.error(f"Критическая ошибка: {e}")

if __name__ == "__main__":
    parse_data()
