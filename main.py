from playwright.sync_api import sync_playwright

def parse_data():
    with sync_playwright() as p:
        browser = p.firefox.launch(headless=True)
        page = browser.new_page()
        page.goto("https://gmgn.ai/sol/address/7XJFKYt7fgABrxXZzkpqS7bUyPwAWaz1WSC2tfGW8nxh")
        page.wait_for_selector(".css-16udrhy", timeout=15000)
        target_div = page.query_selector(".css-16udrhy")
        percentage = target_div.inner_text()
        with open("output.txt", "a") as f:
            f.write(f"{percentage}\n")
        browser.close()

if __name__ == "__main__":
    parse_data()
