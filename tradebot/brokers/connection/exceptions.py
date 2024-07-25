"""
It contains the custom errors used for connections
"""


class TokenGenerationError(Exception):
    """
    Token Generation Not Successful for Broker Connection
    """


class BrokerConnectionError(Exception):
    """
    Error raised when connection to broker is not successful
    """

    def __init__(self, value):
        self.value = value
        super().__init__(f"Connection to Broker Not Successful: Details {self.value}")
