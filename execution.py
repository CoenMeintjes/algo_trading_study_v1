# import numpy as np
# import pandas as pd
# from statsmodels.api import OLS
# from sqlalchemy import create_engine, text
# from sqlalchemy.exc import SQLAlchemyError
# from datetime import timedelta, datetime
# from binance.um_futures import UMFutures
# from binance.error import ClientError
# import math
# import json
# from loguru import logger
# from dotenv import load_dotenv
# import os
# from decimal import Decimal, ROUND_DOWN, ROUND_UP, ROUND_HALF_EVEN

# load_dotenv()

# logger.add('logs/3_execution.log', rotation= '5 MB')

# # API key/secret are required for user data endpoints
# client = UMFutures(key= os.getenv('API_KEY'), secret=os.getenv('SECRET_KEY'))

