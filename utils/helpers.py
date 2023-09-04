# project
from utils.errors import ErrorResponseEnum

class CustomHTTPException(Exception):
    def __init__(self, error_response: ErrorResponseEnum):
        self.error_response = error_response