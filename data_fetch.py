import psycopg2
import pandas as pd
from psycopg2.extras import DictCursor
import datetime
from binance.um_futures import UMFutures
from binance.error import ClientError, ServerError
import requests
import datetime
import pytz
from loguru import logger
from dotenv import load_dotenv
import os

load_dotenv()

client = UMFutures()

logger.add('logs/1_data_fetch.log', rotation= '5 MB')

def data_fetch():
    today = (datetime.now()).date()
    logger.info(f'Initiating data fetch for: {today}')
    # database connection
    # connection = psycopg2.connect(
    #     host= os.getenv('AZDB_HOST'), 
    #     database=  os.getenv('AZDB_NAME_FUT'),
    #     port =  os.getenv('AZDB_PORT'), 
    #     user=  os.getenv('AZDB_USER'), 
    #     password=  os.getenv('AZDB_PASSWORD'),
    #     options= '-c timezone=UTC'
    # )
    with psycopg2.connect(
        host=os.getenv('PG_HOST'),
        database=os.getenv('PG_DATABASE'),
        port=os.getenv('PG_PORT'),
        user=os.getenv('PG_USERNAME'),
        password=os.getenv('PG_PASSWORD'),
        options='-c timezone=UTC'
    ) as connection:
        cursor = connection.cursor(cursor_factory=DictCursor)

    # -------------------------
    # Get a list of all symbols on Binance
    response = requests.get('https://fapi.binance.com/fapi/v1/exchangeInfo')
    data = response.json()

    # Query Margin Symbols
    symbols = [item['symbol'] for item in data['symbols'] if item['status'] == 'TRADING']

    # Flags added based ont the filter above
    trading = 1

    existing_symbols = pd.read_sql(sql="SELECT * FROM asset", con= connection)

    commit_count = 0
    for symbol in symbols:
        if symbol in existing_symbols['symbol'].values:
            logger.info(f'Symbol exists: {symbol}')
            continue
        else:
            try:
                logger.info(f'Inserting asset {symbol} into asset')

                # Finding the symbol data in the provided 'data' dictionary
                symbol_data = next((item for item in data['symbols'] if item['symbol'] == symbol), None)

                # Extracting the lot size filter for the symbol
                lot_size_filter = next((item for item in symbol_data['filters'] if item['filterType'] == 'LOT_SIZE'), None)    
                symbol_lot_size = float(lot_size_filter['minQty'])

                # Extracting the min_notional filter for the symbol
                min_notional_filter = next((item for item in symbol_data['filters'] if item['filterType'] == 'MIN_NOTIONAL'), None)
                symbol_min_notional = float(min_notional_filter['notional'])

                # store in database
                # Update statement if asset info like lot_size or notional min changes
                # cursor.execute('''
                #     INSERT INTO asset (symbol, min_lot_size, trading, min_notional)
                #     VALUES (%s, %s, %s, %s)
                #     ON CONFLICT (symbol) DO UPDATE
                #     SET min_notional = EXCLUDED.min_notional,
                #         min_lot_size = EXCLUDED.min_lot_size;
                # ''', (symbol, symbol_lot_size, trading, symbol_min_notional))
                cursor.execute('''
                    INSERT INTO asset (symbol, min_lot_size, trading, min_notional)
                    VALUES (%s, %s, %s, %s);
                ''', (symbol, symbol_lot_size, trading, symbol_min_notional))

            except psycopg2.errors.UniqueViolation as e:
                connection.rollback()  # Roll back the transaction in case of a unique constraint violation
                logger.info(f"Asset {symbol} already exists in the database. Skipping.")

            except psycopg2.Error as e:
                logger.error(e)

            commit_count += 1
            if commit_count == 10:
                logger.info(f'{commit_count} assets committed')
                connection.commit()  # Commit the transaction
                commit_count = 0

    connection.commit()  # Commit the transaction

    interval = '1d'

    desired_timezone = pytz.timezone('UTC')
    # Parse the start_string as a datetime object with the UTC time zone
    start_string = '2023-11-26 00:00:00 UTC'
    start_date_obj = datetime.datetime.strptime(start_string, '%Y-%m-%d %H:%M:%S %Z').replace(tzinfo=desired_timezone)

    # Convert the adjusted start_date_obj to a Unix timestamp in milliseconds
    start_time = int(start_date_obj.timestamp() * 1000)

    # define request end time | ALWAYS today -1 if I want full day data
    end_string = '2023-11-30 23:59:59 UTC'
    end_date_obj = datetime.datetime.strptime(end_string, '%Y-%m-%d %H:%M:%S %Z').replace(tzinfo=desired_timezone)
    # end_date_obj = end_date_obj.astimezone(desired_timezone)
    end_time = int(end_date_obj.timestamp() * 1000)

    symbol_urls = {}
    for symbol in symbols:
        symbol_urls[symbol] = f"https://fapi.binance.com/fapi/v1/klines?symbol={symbol}&interval={interval}&startTime={start_time}&endTime={end_time}&limit=1000"

    #--------------------
    # count 10 symbols and then commit.
    commit_count = 0
    for symbol in symbols:
        cursor.execute("SELECT id FROM asset WHERE symbol = (%s)", (symbol,))
        rows = cursor.fetchall()
        if rows:
            # Access the 'id' value from the first row (assuming there's only one result)
            asset_id = rows[0][0]
            logger.info(f'asset_id: {asset_id}')
        else:
            # Handle the case when no rows are returned
            logger.info("No matching records found for symbol:", symbol)

        cursor.execute('''
            SELECT MIN(open_time), MAX(close_time) FROM asset_price 
            WHERE asset_id = %s 
        ''', (asset_id,))
        date_range_check = cursor.fetchall()
        date_range_start = date_range_check[0][0]
        logger.info(f'Date range start: {date_range_start}')
        date_range_end = date_range_check[0][1]
        logger.info(f'Date range end: {date_range_end}')

        # if count == 0:
        if date_range_start is None or date_range_start > start_date_obj or date_range_end < end_date_obj:
            try:
                data = client.continuous_klines(pair= symbol, contractType='PERPETUAL', interval=interval, startTime=start_time, endTime= end_time, limit=1000)
                bars_fetched = len(data)
                logger.info(f'No. of Candles Fetched: {bars_fetched}')


                data_to_insert = []
                for row in data:
                    open_time_sast = datetime.datetime.utcfromtimestamp(row[0] / 1000)
                    open_time = open_time_sast.replace(tzinfo=pytz.utc).astimezone(desired_timezone)
                    close_time_sast = datetime.datetime.utcfromtimestamp(row[6] / 1000)
                    close_time = close_time_sast.replace(tzinfo=pytz.utc).astimezone(desired_timezone)
                    data_to_insert.append((asset_id, open_time, row[1], row[2], row[3], row[4], row[5], close_time))

                sql = '''
                    INSERT INTO asset_price (asset_id, open_time, open, high, low, close, volume, close_time) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                '''

                logger.info(f'Executing {symbol} data for ingestion into asset_price')
                cursor.executemany(sql, data_to_insert)
                commit_count += 1

                if commit_count == 20:
                    connection.commit()  # Commit the transaction
                    logger.info(f'{commit_count} x symbol data committed to asset_price table')
                    commit_count = 0  

            except psycopg2.Error as e:
                connection.rollback()
                logger.error(f"PostgreSQL Error: {e}")
            except IndexError as e:
                logger.error(f"Index Error: {e}")
                # Handle specific index error scenario
            except ClientError as e:
                logger.error(f"Client Error: {e}")
                # Handle specific client error scenario
            except TypeError as e:
                logger.error(f"Type Error: {e}")
                # Handle specific type error scenario
            except Exception as e:
                logger.error(f"Unexpected Error: {e}")
                connection.rollback()

        else:
            logger.info('Record exists.')

    # Commit any remaining data
    if commit_count > 0:
        try:
            connection.commit()
            logger.info(f'{commit_count} x symbol data committed to asset_price table')
        except psycopg2.Error as e:
            connection.rollback()
            logger.error(f"Error during final commit: {e}")
        
    cursor.execute(f'SELECT MIN(open_time) AS date_range_start, MAX(close_time) AS date_range_end FROM asset_price')
    date_range = cursor.fetchone()
    db_opencandle = date_range[1]
    db_closecandle = date_range[2]
    logger.info(f"DB date range | {db_opencandle} --> {db_closecandle}")

    # close the db connection
    connection.close()

    logger.info(f'End of data_fetch for: {today}')
    logger.info('-' * 50)
    logger.info('\n')

