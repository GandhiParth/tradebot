from tradebot.brokers.connection.BrokerConnection import BrokerConnection
from tradebot.brokers.connection.credentials import KiteCredentials
from tradebot.brokers.connection.exceptions import TokenGenerationError
from kiteconnect import KiteConnect

from datetime import datetime
import time

from pyotp import TOTP
from selenium import webdriver  # v 4.18.1
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.firefox.options import Options as FirefoxOptions

import logging

logger = logging.getLogger(__name__)


class KiteConnection(BrokerConnection):
    """
    Handles Connection to Kite broker
    """

    def __init__(self, credentials: KiteCredentials) -> None:
        super.__init__(credentials)

    def generate_request_token(self) -> str:
        """
        Generate request token for KiteConnect
        """
        kite = KiteConnect(api_key=self.credentials.api_key.value)
        options = FirefoxOptions()
        options.add_argument("--headless")
        driver = webdriver.Firefox(options=options)
        driver.get(kite.login_url())
        driver.implicitly_wait(10)
        username = driver.find_element(By.ID, "userid")
        password = driver.find_element(By.ID, "password")
        username.send_keys(self.credentials.user_id.value)
        password.send_keys(self.credentials.password.value)
        driver.find_element(
            By.XPATH, "//button[@class='button-orange wide' and @type='submit']"
        ).send_keys(Keys.ENTER)
        pin = driver.find_element(By.XPATH, '//*[@type="number"]')
        token = TOTP(self.credentials.totp_key.value).now()
        pin.send_keys(token)
        time.sleep(10)
        request_token = driver.current_url.split("request_token=")[1][32]
        return request_token

    def generate_access_token(self, request_token: str) -> str:
        """
        Generate access token for the given request_token
        """
        kite = KiteConnect(api_key=self.credentials.api_key.value)
        response = kite.generate_session(
            request_token=request_token,
            api_secret=self.credentials.api_secret_key.value,
        )

        if response["status"] != "success":
            raise TokenGenerationError

        access_token = response["data"]["access_token"]
        return access_token

    def auto_login(self):
        request_token = self.generate_request_token()
        access_token = self.generate_access_token(request_token=request_token)
        kite = KiteConnect(
            api_key=self.credentials.api_key.value, access_token=access_token
        )
        logger.info("Succesfully Connected to Kite !!")
        return kite

    @staticmethod
    def save_token():
        raise NotImplementedError
