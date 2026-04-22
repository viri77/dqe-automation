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
        try:
            return self.driver
        except  WebDriverException as e:
                print(f"launch webdriver exception: {e}")
        except  TimeoutException as e:
                print(f"timeout exception: {e}")
                raise

    def __exit__(self, exc_type, exc_value, traceback):
            if self.driver:
                self.driver.quit()


if __name__ == "__main__":
    with SeleniumWebDriverContextManager() as driver:
        file_path = "report.html"
        abs_path = os.path.abspath(file_path)
        url = f"file:///{abs_path.replace(os.sep, '/')}"
        driver.get(url)
        time.sleep(5)
        try:
            table_element = driver.find_element(By.CLASS_NAME, "table")
            columns = table_element.find_elements(By.CLASS_NAME, "y-column")
        except NoSuchElementException as e:
            print(f"No SuchElement : {e}")
            raise

        headers = []
        values = []

        for col in columns:
            header_blocks = col.find_elements(By.ID, 'header')
            for block in header_blocks:
                headers.append(block.text)
            if col.get_attribute("id") != "header":
                values.append(col.text)

        data_columns = [col.split('\n') for col in values]

        num_rows = min((len(col)-1) for col in data_columns) if data_columns else 0

#writing table to CSV

        csv_file = "table.csv"
        with open(csv_file, mode="w", newline='', encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(headers)
            for i in range(num_rows):
                row = [data_columns[j][i] for j in range(len(data_columns))]
                writer.writerow(row)

        print(f"Saved to: {csv_file}")

        #chart

        chart = driver.find_element(By.CLASS_NAME, "pielayer")
        chart.screenshot("screenshot0.png")

        #pychart screenshot initial state

        texts = driver.find_elements(By.CSS_SELECTOR, "text.slicetext")
        if not texts:
            raise NoSuchElementException("No slicetext elements found")
        data = []
        for t in texts:
            raw = t.get_attribute("data-unformatted")
            if raw:
                parts = raw.replace('&lt;br&gt;', '\n').replace('<br>', '\n').split('\n')
                if len(parts) >= 2:
                    label = parts[0].strip()
                    value = parts[1].strip()
                    data.append([label, value])

        with open("doughnut0.csv", mode="w", newline='', encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["Facility Type", "Min average time spent"])
            writer.writerows(data)
        try:
            donut = driver.find_elements(By.CLASS_NAME, "legendtoggle")
            labels = driver.find_elements(By.CLASS_NAME, "legendtext")
        except NoSuchElementException as e :
            print("no donut found" ,{e})
            raise
        for i in range(len(labels)):
            try:
                donut[i].click()
                time.sleep(2)
                try:
                    chart = driver.find_element(By.CLASS_NAME, "pielayer")
                    chart.screenshot(f"screenshot{i + 1}.png")
                except (NoSuchElementException, WebDriverException):
                    driver.save_screenshot(f"screenshot{i + 1}.png")

                time.sleep(1)
                texts = driver.find_elements(By.CSS_SELECTOR, "text.slicetext")
                data = []
                if texts:
                    for t in texts:
                        raw = t.get_attribute("data-unformatted")
                        if raw:
                            parts = raw.replace('&lt;br&gt;', '\n').replace('<br>', '\n').split('\n')
                            if len(parts) >= 2:
                                label = parts[0].strip()
                                value = parts[1].strip()
                                data.append([label, value])
                        print(raw)
                    print(data)
                else:
                    print("Pie chart disappeared, writing only header.")

                with open(f"doughnut{i + 1}.csv", mode="w", newline='', encoding="utf-8") as f:
                    writer = csv.writer(f)
                    writer.writerow(["Facility Type", "Min average time spent"])
                    if data:
                        writer.writerows(data)
                    # Если data пустой, будет только шапка

                time.sleep(5)
            except Exception as e:
                print(f"Error during filter click {i}: {e}")