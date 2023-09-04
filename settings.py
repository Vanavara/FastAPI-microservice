# stdlib
import os

# thirdparty
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.environ["DATABASE_URL"]
YEARS_RANGE = (2022, 2027)
