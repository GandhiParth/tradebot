import os

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.firefox.options import Options as FirefoxOptions
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait


def get_corp_actns(symbl: str):
    os.environ["PATH"] += r"/home/parthgandhi/AlgoTrading"
    options = FirefoxOptions()
    # options.add_argument("--headless")
    driver = webdriver.Firefox(options=options)
    driver.get("https://www.bseindia.com/index.html")
    driver.implicitly_wait(60)
    search_box = driver.find_element(By.ID, "getquotesearch")
    search_box.send_keys(symbl)
    search_box.send_keys(Keys.SPACE)
    search_box.send_keys(Keys.BACKSPACE)
    search_box.send_keys(Keys.ARROW_DOWN)
    search_box.send_keys(Keys.ENTER)
    corp_actn_element = WebDriverWait(driver, 30).until(
        EC.element_to_be_clickable(
            (By.XPATH, "//a[@class='single-item' and contains(text(), 'Corp Actions')]")
        )
    )
    corp_actn_element.send_keys(Keys.ENTER)

    try:

        arrow_elements = WebDriverWait(driver, 30).until(
            EC.visibility_of_all_elements_located(
                (By.XPATH, "//i[@class='fa fa-arrow-circle-right']")
            )
        )

        if arrow_elements:
            WebDriverWait(driver, 30).until(
                EC.element_to_be_clickable(
                    (By.XPATH, "//i[@class='fa fa-arrow-circle-right']")
                )
            )
            arrow_elements[0].click()
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            download_icon = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable(
                    (By.XPATH, "//i[@class='fa fa-download iconfont']")
                )
            )

            download_icon.click()
        else:
            print("No arrow elements found")
    except Exception as e:
        print("Error:", e)
