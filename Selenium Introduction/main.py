import selenium
import time
import os
import csv


from selenium import webdriver
from selenium.webdriver.chrome.webdriver import WebDriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium.common.exceptions import WebDriverException

class SeleniumWebDriverContextManager:
    def __init__(self):
        self.driver = webdriver.Chrome()
        self.driver.maximize_window()

    def __enter__(self):
        return self.driver


    def __exit__(self, exc_type, exc_value, traceback):
            if self.driver:
                self.driver.quit()


def extract_table(driver) -> tuple[list[str], list[list[str]]]:
    table_element = driver.find_element(By.CLASS_NAME, "table")
    columns = table_element.find_elements(By.CLASS_NAME, "y-column")

    headers = []
    values = []

    for col in columns:
        header_blocks = col.find_elements(By.ID, 'header')
        for block in header_blocks:
            headers.append(block.text)
        if col.get_attribute("id") != "header":
            values.append(col.text)

    data_columns = [col.split('\n') for col in values]
    num_rows = min((len(col) - 1) for col in data_columns) if data_columns else 0

    rows = []
    for i in range(num_rows):
        row = [data_columns[j][i] for j in range(len(data_columns))]
        rows.append(row)

    return headers, rows


def save_csv(filename: str, headers: list[str], rows: list[list[str]]):
    with open(filename, mode="w", newline='', encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(rows)
    print(f"Saved to: {filename}")


def extract_doughnut_data(driver) -> list[list[str]]:
    texts = driver.find_elements(By.CSS_SELECTOR, "text.slicetext")
    data = []

    for t in texts:
        raw = t.get_attribute("data-unformatted")
        if raw:
            parts = raw.replace('&lt;br&gt;', '\n').replace('<br>', '\n').split('\n')
            if len(parts) >= 2:
                label = parts[0].strip()
                value = parts[1].strip()
                data.append([label, value])

    return data

def take_screenshot(driver, filename: str):
    driver.save_screenshot(filename)
    print(f"screenshot saved: {filename}")



def process_legend_filters(driver, doughnut_headers: list[str]):
    toggles = driver.find_elements(By.CLASS_NAME, "legendtoggle")

    for i, toggle in enumerate(toggles):
        try:
            toggle.click()
            time.sleep(2)

            take_screenshot(driver, f"screenshot{i + 1}.png")

            time.sleep(1)
            data = extract_doughnut_data(driver)
            save_csv(f"doughnut{i + 1}.csv", doughnut_headers, data)

            time.sleep(5)
        except Exception as e:
            print(f"Error during filter click {i}: {e}")





if __name__ == "__main__":
    with SeleniumWebDriverContextManager() as driver:
        report_file_path = os.path.abspath("report.html")
        url = f"file:///{report_file_path}"
        driver.get(url)
        time.sleep(5)

        try:
            headers, rows = extract_table(driver)
            save_csv("table.csv", headers, rows)
        except NoSuchElementException as e:
            print(f"Table not found: {e}")
            raise

        take_screenshot(driver, "screenshot0.png")

        doughnut_headers = ["Facility Type", "Min average time spent"]
        data = extract_doughnut_data(driver)

        if not data:
            raise NoSuchElementException("No slicetext elements found")

        save_csv("doughnut0.csv", doughnut_headers, data)

        process_legend_filters(driver, doughnut_headers)