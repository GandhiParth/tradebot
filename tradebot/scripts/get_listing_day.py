import argparse
import csv
import logging
import os

import polars as pl
from ratelimit import limits, sleep_and_retry
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options as FirefoxOptions
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait


def setup_logger(log_file_path):
    logger = logging.getLogger("NSELogger")
    logger.setLevel(logging.INFO)

    # Create a file handler that logs to the specified file
    file_handler = logging.FileHandler(log_file_path, mode="a")  # 'a' for append mode
    file_handler.setLevel(logging.INFO)

    # Create a console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)

    # Create a formatter and set it for the handlers
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s : %(message)s"
    )
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    # Add the handlers to the logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger


@sleep_and_retry
@limits(calls=1, period=20)
def get_listing_date(symbol: str, logger):
    """
    get listing day for the symbol provided from NSE website
    """
    try:
        options = FirefoxOptions()
        options.add_argument("--headless")
        os.environ["PATH"] += "E:/"
        driver = webdriver.Firefox(options=options)
        driver.maximize_window()
        driver.get(f"https://www.nseindia.com/get-quotes/equity?symbol={symbol}")
        driver.implicitly_wait(15)
        trade_information_element = driver.find_element(By.ID, "Trade_Information_pg")
        trade_information_element.click()
        date_of_listing_element = driver.find_element(By.ID, "Date_of_Listing")
        # date_of_listing_element = WebDriverWait(driver, 20).until(
        #     EC.presence_of_element_located((By.ID, "Date_of_Listing"))
        # )
        date_of_listing = date_of_listing_element.find_element(
            By.XPATH, "./following-sibling::td"
        ).text
        logger.info(f"{symbol}:{date_of_listing}")
        driver.quit()
        return date_of_listing
    except Exception as e:
        driver.quit()
        logger.error(e)


def append_to_csv(file_path, data, logger):
    file_exists = os.path.isfile(file_path)
    try:
        with open(file_path, "a", newline="") as csvfile:
            fieldnames = ["symbol", "date_of_listing"]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            if not file_exists:
                writer.writeheader()  # Write header only if file doesn't exist

            writer.writerow(data)
        logger.info(f"Data appended to CSV file: {data}")
    except Exception as e:
        logger.error(f"Error writing to CSV file: {e}")


# def parse_args():
#     """
#     Parse command-line arguments.
#     """
#     parser = argparse.ArgumentParser(
#         description="Get listing date of a stock symbol from NSE."
#     )
#     parser.add_argument(
#         "symbol", type=str, help="Stock symbol to get the listing date for"
#     )
#     return parser.parse_args()


if __name__ == "__main__":
    instrument_list = (
        pl.read_csv(
            "/home/parthgandhi/Projects/tradebot/test_scripts/instrument_list_correct.csv"
        )
        .get_columns()[0]
        .to_list()
    )

    log_file_path = "nse_log.log"
    logger = setup_logger(log_file_path)
    csv_file_path = "listing_dates.csv"
    failed_path = "failed.csv"
    for symbol in instrument_list:

        count = 0
        date_of_listing = None
        while count < 5:
            date_of_listing = get_listing_date(symbol, logger)
            count += 1

            if date_of_listing is not None:
                if len(date_of_listing) == 11:
                    data = {"symbol": symbol, "date_of_listing": date_of_listing}
                    append_to_csv(csv_file_path, data, logger)
                    break

        if date_of_listing is None:
            data = {"symbol": symbol, "date_of_listing": date_of_listing}
            append_to_csv(failed_path, data, logger)
        elif len(date_of_listing) != 11:
            data = {"symbol": symbol, "date_of_listing": date_of_listing}
            append_to_csv(failed_path, data, logger)
