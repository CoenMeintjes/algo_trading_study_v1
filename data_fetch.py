from datetime import datetime
from binance.um_futures import UMFutures
from binance.error import ClientError
import requests
import pytz
from loguru import logger
from sqlalchemy import create_engine, func
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker
from mapped_model import Asset, AssetPrice

# Set up logger
logger.add('logs/1_data_fetch.log', rotation='5 MB')

def data_fetch(start: str, end: str, connection_string):
    client = UMFutures()
    today = datetime.now().date()
    
    # DATABASE CONNECTION
    engine = create_engine(f'postgresql://{connection_string}')
    Session = sessionmaker(bind=engine)
    session = Session()
    logger.info('Connected to database')
    
    try:
        # Get list of all symbols on Binance
        response = requests.get('https://fapi.binance.com/fapi/v1/exchangeInfo')
        data = response.json()
        symbols = [item['symbol'] for item in data['symbols'] if item['status'] == 'TRADING']
        trading = 1

        existing_symbols = session.query(Asset.symbol).all()

        for symbol in symbols:
            if (symbol,) in existing_symbols:
                logger.info(f'Symbol exists: {symbol}')
                continue
            else:
                try:
                    logger.info(f'Inserting asset {symbol} into asset table')
                    symbol_data = next((item for item in data['symbols'] if item['symbol'] == symbol), None)
                    lot_size_filter = next((item for item in symbol_data['filters'] if item['filterType'] == 'LOT_SIZE'), None)    
                    symbol_lot_size = float(lot_size_filter['minQty'])
                    min_notional_filter = next((item for item in symbol_data['filters'] if item['filterType'] == 'MIN_NOTIONAL'), None)
                    symbol_min_notional = float(min_notional_filter['notional'])
                    new_asset = Asset(symbol=symbol, min_lot_size=symbol_lot_size, trading=trading, min_notional=symbol_min_notional)
                    session.add(new_asset)
                except IntegrityError:
                    session.rollback()
                    logger.info(f"Symbol {symbol} already inserted by another process")
                except Exception as e:
                    session.rollback()
                    logger.error(f"Error inserting {symbol}: {e}")
        session.commit()
        
        interval = '1d'
        desired_timezone = pytz.timezone('UTC')
        
        start_string = f'{start} 00:00:00 UTC'
        start_date_obj = datetime.strptime(start_string, '%Y-%m-%d %H:%M:%S %Z').replace(tzinfo=desired_timezone)
        start_time = int(start_date_obj.timestamp() * 1000)
        
        end_string = f'{end} 23:59:59 UTC'
        end_date_obj = datetime.strptime(end_string, '%Y-%m-%d %H:%M:%S %Z').replace(tzinfo=desired_timezone)
        end_time = int(end_date_obj.timestamp() * 1000)

        for symbol in symbols:
            asset = session.query(Asset).filter(Asset.symbol == symbol).first()
            if asset:
                asset_id = asset.id
                logger.info(f'asset_id: {asset_id}')
                date_range_check = (
                    session.query(func.min(AssetPrice.open_time), func.max(AssetPrice.close_time))
                    .filter(AssetPrice.asset_id == asset_id)
                    .first()
                )
                date_range_start, date_range_end = date_range_check
                logger.info(f'Date range start: {date_range_start}')
                logger.info(f'Date range end: {date_range_end}')
                if date_range_start is None or date_range_start > start_date_obj or date_range_end < end_date_obj:
                    try:
                        data = client.continuous_klines(pair=symbol, contractType='PERPETUAL', interval=interval, startTime=start_time, endTime=end_time, limit=1000)
                        bars_fetched = len(data)
                        logger.info(f'No. of Candles Fetched: {bars_fetched}')

                        data_to_insert = [
                            {
                                'asset_id': asset_id,
                                'open_time': datetime.utcfromtimestamp(row[0] / 1000).astimezone(desired_timezone),
                                'open': row[1],
                                'high': row[2],
                                'low': row[3],
                                'close': row[4],
                                'volume': row[5],
                                'close_time': datetime.utcfromtimestamp(row[6] / 1000).astimezone(desired_timezone)
                            }
                            for row in data
                        ]

                        try:
                            session.bulk_insert_mappings(AssetPrice, data_to_insert)
                            session.commit()
                            logger.info(f'Executing {symbol} data for ingestion into asset_price')
                        except IntegrityError as e:
                            session.rollback()
                            logger.error(f'Integrity Error while inserting data: {e}')
                        except Exception as e:
                            session.rollback()
                            logger.error(f'Error while inserting data: {e}')
                    except IndexError as e:
                        logger.error(f'Index Error: {e}')
                    except ClientError as e:
                        logger.error(f'Client Error: {e}')
                    except TypeError as e:
                        logger.error(f'Type Error: {e}')
                    except Exception as e:
                        logger.error(f'Unexpected Error: {e}')
                        session.rollback()
                else:
                    logger.info('Record exists.')
            else:
                logger.info(f'No matching records found for symbol: {symbol}')

        date_range = (
            session.query(func.min(AssetPrice.open_time).label('date_range_start'), func.max(AssetPrice.close_time).label('date_range_end'))
            .one()
        )
        db_opencandle, db_closecandle = date_range.date_range_start, date_range.date_range_end

        logger.info('-' * 50)
        logger.info(f"DB date range | {db_opencandle} --> {db_closecandle}")
        logger.info(f'End of data_fetch for: {today}')
        logger.info('-' * 50)
        logger.info('-' * 50)

    except Exception as e:
        logger.error(f'Error with database connection | {e}')

    finally:
        session.close()