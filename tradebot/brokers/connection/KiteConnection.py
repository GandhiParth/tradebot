import logging
import os
import time

from kiteconnect import KiteConnect
from pyotp import TOTP
from selenium import webdriver  # v 4.18.1
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.firefox.options import Options as FirefoxOptions

from tradebot.brokers.connection.BrokerConnection import BrokerConnection
from tradebot.brokers.connection.exceptions import (
    BrokerConnectionError,
    TokenGenerationError,
)

logger = logging.getLogger(__name__)


class KiteConnection(BrokerConnection):
    """
    Handles Connection to Kite broker
    """

    def __init__(self, credentials_yaml_file: str) -> None:
        super().__init__(credentials_yaml_file)

    def _generate_request_token(self) -> str:
        """
        Generate request token for KiteConnect
        """
        kite = KiteConnect(api_key=self.credentials["api_key"])
        options = FirefoxOptions()
        options.add_argument("--headless")
        driver = webdriver.Firefox(options=options)
        driver.get(kite.login_url())
        driver.implicitly_wait(10)
        username = driver.find_element(By.ID, "userid")
        password = driver.find_element(By.ID, "password")
        username.send_keys(self.credentials["user_id"])
        password.send_keys(self.credentials["password"])
        driver.find_element(
            By.XPATH, "//button[@class='button-orange wide' and @type='submit']"
        ).send_keys(Keys.ENTER)
        pin = driver.find_element(By.XPATH, '//*[@type="number"]')
        token = TOTP(self.credentials["totp_key"]).now()
        pin.send_keys(token)
        time.sleep(10)
        request_token = driver.current_url.split("request_token=")[1][32]
        return request_token

    def _generate_access_token(self, request_token: str) -> str:
        """
        Generate access token for the given request_token
        """
        kite = KiteConnect(api_key=self.credentials["api_key"])
        response = kite.generate_session(
            request_token=request_token,
            api_secret=self.credentials["api_secret_key"],
        )

        if response["status"] != "success":
            raise TokenGenerationError

        access_token = response["data"]["access_token"]
        return access_token

    def auto_login(self) -> KiteConnect:
        """
        automatically logins to Kite
        """
        request_token = self._generate_request_token()
        access_token = self._generate_access_token(request_token=request_token)
        try:
            kite = KiteConnect(
                api_key=self.credentials["api_key"], access_token=access_token
            )
            logger.info("Kite Connection object created successfully")

            os.environ["kite_api_key"] = self.credentials["api_key"]
            os.environ["kite_access_token"] = access_token
            logger.info("Kite api_key and access_token saved as os variables")

            return kite
        except Exception as e:
            logger.error(e)
            raise BrokerConnectionError(e) from e
