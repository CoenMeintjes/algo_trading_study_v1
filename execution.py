# %%
import pandas as pd
from statsmodels.api import OLS
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from datetime import timedelta, datetime
from binance.um_futures import UMFutures
from binance.error import ClientError
from decimal import Decimal, ROUND_UP, ROUND_HALF_EVEN
import logging

def execution_model(binance_api: str, binance_secret: str, connection_string):
    # Get server timestamp
    logging.info('\n')
    logging.info('-' * 50)
    client = UMFutures(key= binance_api, secret= binance_secret)
    logging.info(f'Time at runtime: {client.time()}')

    # Get account and balance information
    account = client.account()
    account = account.get('assets', [])
    positions = [asset for asset in account if float(asset['walletBalance']) > 0]
    account_positions = pd.DataFrame(positions)
    account_positions = account_positions[['asset', 'walletBalance']]
    logging.info(f'\n{account_positions}')

    ### DATABASE CONNECTION
    try:
        engine = create_engine(f'postgresql://{connection_string}')
        logging.info('Connected to database')

    except Exception as e:
        logging.error(f'Error with database connection | {e}')
        
    ### DATA PREPERATION
    # Select the month start and end that the strategy is running
    insample_start = datetime(2023, 12, 26)
    insample_end = datetime(2024, 1, 25)

    # Get the current date without time
    today = (datetime.now()).date()
    yesterday = today - timedelta(days= 1)

    backdata_start = pd.to_datetime(insample_start) - timedelta(days = 21)
    backdata_end = pd.to_datetime(insample_start) - timedelta(days = 1)

    logging.info(f'Running strategy for month of {insample_start} --> {insample_end}')
    logging.info('-' * 50)

    logging.info('Query database for trading pairs...')
    trading_pairs_query = text('''
        SELECT 
            asset1.symbol AS symbol_1,
            asset2.symbol AS symbol_2,
            tp.trainset_end
        FROM trading_pairs AS tp
        JOIN asset AS asset1 ON tp.symbol_1_id = asset1.id
        JOIN asset AS asset2 ON tp.symbol_2_id = asset2.id
        WHERE tp.trainset_end = :trainset_end;
    ''')

    trading_pairs = pd.read_sql(trading_pairs_query, engine, params={'trainset_end': backdata_end})

    # Initialize a list to store backtest statistics
    backtest_stats = []

    # Set the initial account balance
    initial_account_balance = 1000
    account_balance = initial_account_balance

    # Set position parameters
    position_size = account_balance * 0.04 # This allows for full allocation to 25 pairs

    # Initialize an empty DataFrame to store the cumulative P&L for all pairs
    all_pairs_dd = pd.DataFrame(columns=['cumulative_dd'])

    # dict for all pair postitions
    all_positions = pd.DataFrame()

    # Initialize a dictionary to store cumulative pnl DataFrames for each pair
    all_pairs_pnl_dict = {}

    # Transaction fee rate (0.1%)
    transaction_fee_rate = 0.001  # 0.1%

    # Define the rolling window period
    rolling_window_size = 20

    # Iterate through each pair in the 'backtest_pairs' DataFrame
    for index, row in trading_pairs.iterrows():
        # Extract symbols for the pair
        pair_1 = row['symbol_1']
        pair_2 = row['symbol_2']
        logging.info('\n')
        logging.info(f'Now processing {pair_1}-{pair_2}')
        logging.info('-' * 40)

        symbol_1 = pair_1
        symbol_2 = pair_2

        symbol_1_id_query = text('SELECT id FROM asset WHERE symbol = :symbol_1')
        symbol_2_id_query = text('SELECT id FROM asset WHERE symbol = :symbol_2')

        with engine.connect() as connection:
            symbol_1_id_result = connection.execute(symbol_1_id_query, {'symbol_1': symbol_1}).fetchone()
            symbol_2_id_result = connection.execute(symbol_2_id_query, {'symbol_2': symbol_2}).fetchone()

        symbol_1_id = symbol_1_id_result[0] 
        symbol_2_id = symbol_2_id_result[0]

        pair_data_query = text(f'''
            SELECT
                ap_symbol_1.open_time AS date,
                ap_symbol_1.close AS close_{symbol_1},
                ap_symbol_2.close AS close_{symbol_2}
            FROM
                asset_price AS ap_symbol_1
            INNER JOIN
                asset AS a_symbol_1 ON ap_symbol_1.asset_id = a_symbol_1.id
            INNER JOIN
                asset_price AS ap_symbol_2
                ON ap_symbol_1.open_time = ap_symbol_2.open_time
                AND ap_symbol_1.asset_id <> ap_symbol_2.asset_id
            INNER JOIN
                asset AS a_symbol_2 ON ap_symbol_2.asset_id = a_symbol_2.id
            WHERE
                a_symbol_1.symbol = :symbol_1
                    AND a_symbol_2.symbol = :symbol_2
                    AND ap_symbol_1.open_time BETWEEN :backdata_start AND :yesterday
            ORDER BY
                ap_symbol_1.open_time;
        ''')

        df = pd.read_sql_query(pair_data_query, engine, params={'symbol_1': symbol_1, 'symbol_2': symbol_2, 'backdata_start': backdata_start, 'yesterday': yesterday})

        # Determine the hedge ratio on the training set using OLS regression
        model = OLS(df[f'close_{(pair_1).lower()}'], df[f'close_{(pair_2).lower()}'])
        results = model.fit()
        hedge_ratio = results.params.iloc[0]
        df['hedge_ratio'] = hedge_ratio

        # Calculate the spread for both the training and test sets based on the hedge ratio
        spread = df.loc[:, f'close_{(pair_1).lower()}'] - hedge_ratio * df.loc[:, f'close_{(pair_2).lower()}']

        df['spread'] = spread

        # Calculate the mean and standard deviation of the spread on the training set
        spread_mean = spread.mean()
        spread_std = spread.std()

        # Create a 'zscore' column to standardize the spread using the mean and standard deviation
        df['zscore'] = (spread - spread_mean) / spread_std

        # Calculate the rolling mean and standard deviation for the spread
        spread_mean_rolling = df['spread'].shift().rolling(window=rolling_window_size).mean()
        spread_std_rolling = df['spread'].shift().rolling(window=rolling_window_size).std()
        
        df['rolling_mean'] = spread_mean_rolling
        df['rolling_std'] = spread_std_rolling

        # Calculate dynamic entry and exit thresholds based on the rolling statistics
        entry_threshold = spread_std_rolling * 0.5 # For example, enter when the current z-score is 2 standard deviations away from the rolling mean
        exit_threshold = spread_mean_rolling * 0.25 # For example, exit when the current z-score is 0.5 standard deviations away from the rolling mean

        df['entry_threshold'] = entry_threshold
        df['exit_threshold'] = exit_threshold
        
        # Initialize columns for trading positions for both assets, including long and short positions
        df[f'pos_{(pair_1).lower()}_long'] = 0
        df[f'pos_{(pair_2).lower()}_long'] = 0
        df[f'pos_{(pair_1).lower()}_short'] = 0
        df[f'pos_{(pair_2).lower()}_short'] = 0

        df['in_position'] = 0
        # Set trading positions based on z-score conditions
        for i in range(1, len(df)):
            zscore = df.at[i-1,'zscore']
            entry_threshold = df.at[i-1, 'entry_threshold']
            exit_threshold = df.at[i-1, 'exit_threshold']

            if df.at[i, 'in_position'] == 0 and zscore <= -entry_threshold:
                df.loc[i:, (f'pos_{pair_1.lower()}_long', f'pos_{pair_2.lower()}_short')] = [1, -1]
                df.loc[i:, 'in_position'] = 1
                continue

            if df.at[i, 'in_position'] == 1 and zscore >= exit_threshold:
                df.loc[i:, (f'pos_{pair_1.lower()}_long', f'pos_{pair_2.lower()}_short')] = 0
                df.loc[i:, 'in_position']  = 0 
                continue

            if df.at[i, 'in_position'] == 0 and zscore >= entry_threshold:
                df.loc[i:, (f'pos_{(pair_1).lower()}_short', f'pos_{(pair_2).lower()}_long')] = [-1, 1]
                df.loc[i:, 'in_position']  = -1
                continue

            if df.at[i, 'in_position'] == -1 and zscore <= -exit_threshold:
                df.loc[i:, (f'pos_{(pair_1).lower()}_short', f'pos_{(pair_2).lower()}_long')] = 0 
                df.loc[i:, 'in_position']  = 0 
                continue
            
        # Spread position | Extract long and short trading positions for both assets
        long_spread = df.loc[:, (f'pos_{(pair_1).lower()}_long', f'pos_{(pair_2).lower()}_short')]
        short_spread = df.loc[:, (f'pos_{(pair_1).lower()}_short', f'pos_{(pair_2).lower()}_long')]
        
        # Calculate position change
        spread_change = (long_spread != long_spread.shift(1)).any(axis=1) | (short_spread != short_spread.shift(1)).any(axis=1)
        df['position_change'] = spread_change
        
        logging.info('execution model ran up to trade execution')
        # -------------------
        # Trade execution
        # -------------------
        '''TODO current execution will only be able to handle USDT / USD denominated symbols other base currencies will need
        to be handled by getting the symbol price and then converting to USD
        '''
        # set parameters
        total_allocation = position_size
        symbol_allocation = total_allocation / 2
        symbol_alloction_threshold = symbol_allocation * 1.15 # set to 15% based on log check that max excess above allocations was around 11.10
        symbol_alloction_threshold = Decimal(symbol_alloction_threshold).quantize(Decimal('0.00'), rounding=ROUND_HALF_EVEN)

        # query all orders for this pair in this month
        position_query = text('''
            SELECT pair, symbol, update_time, orig_qty, status, spread, pair_order
            FROM orders
            WHERE pair = :pair_symbol
            ORDER BY update_time ASC;
        ''')

        positions = pd.read_sql(position_query, engine, params={'pair_symbol': f'{symbol_1}-{symbol_2}'})

        # get the current asset_price 
        symbol_1_price = (client.ticker_price(f'{symbol_1}'))
        symbol_1_price = Decimal(symbol_1_price['price'])

        symbol_2_price = (client.ticker_price(f'{symbol_2}'))
        symbol_2_price = Decimal(symbol_2_price['price'])

        # --------------------
        # Query asset lot sizes
        # --------------------
        query_lot_size_1 = text('''
            SELECT min_lot_size, min_notional
            FROM asset
            WHERE symbol = :symbol_1
        ''')

        query_lot_size_2 = text('''
            SELECT min_lot_size, min_notional
            FROM asset
            WHERE symbol = :symbol_2
        ''')

        with engine.connect() as connection:
            result_1 = connection.execute(query_lot_size_1, {'symbol_1': symbol_1}).fetchone()
            symbol_1_lot_size = result_1[0]
            symbol_1_min_notional = result_1[1]  # Extract min_notional value for symbol_1

            result_2 = connection.execute(query_lot_size_2, {'symbol_2': symbol_2}).fetchone()
            symbol_2_lot_size = result_2[0]
            symbol_2_min_notional = result_2[1]  # Extract min_notional value for symbol_2         

        # Calculate lot_sizes based on minQty 
        if symbol_1_lot_size < 1:
            symbol_1_lot_size = Decimal(symbol_1_lot_size) 
            symbol_allocation = Decimal (symbol_allocation)
            symbol_1_price = Decimal(symbol_1_price)
            symbol_1_target_quantity = ((symbol_allocation / symbol_1_price) / symbol_1_lot_size) * symbol_1_lot_size
            symbol_1_target_quantity = symbol_1_target_quantity.quantize(Decimal(str(symbol_1_lot_size)), rounding= ROUND_UP)

        else:
            symbol_1_lot_size = 1
            symbol_1_lot_size = Decimal(symbol_1_lot_size) 
            symbol_allocation = Decimal (symbol_allocation)
            symbol_1_price = Decimal(symbol_1_price)
            symbol_1_target_quantity = ((symbol_allocation / symbol_1_price) / symbol_1_lot_size) * symbol_1_lot_size
            symbol_1_target_quantity = symbol_1_target_quantity.quantize(Decimal(str(symbol_1_lot_size)), rounding= ROUND_HALF_EVEN)

        if symbol_2_lot_size < 1:
            symbol_2_lot_size = Decimal(symbol_2_lot_size) 
            symbol_allocation = Decimal (symbol_allocation)
            symbol_2_price = Decimal(symbol_2_price)
            symbol_2_target_quantity = ((symbol_allocation / symbol_2_price) / symbol_2_lot_size) * symbol_2_lot_size
            symbol_2_target_quantity = symbol_2_target_quantity.quantize(Decimal(str(symbol_2_lot_size)),rounding= ROUND_UP)

        else:
            symbol_2_lot_size = 1
            symbol_2_lot_size = Decimal(symbol_2_lot_size) 
            symbol_allocation = Decimal (symbol_allocation)
            symbol_2_price = Decimal(symbol_2_price)
            symbol_2_target_quantity = ((symbol_allocation / symbol_2_price) / symbol_2_lot_size) * symbol_2_lot_size
            symbol_2_target_quantity = symbol_2_target_quantity.quantize(Decimal(str(symbol_2_lot_size)), rounding= ROUND_HALF_EVEN)

        symbol_1_order_value = symbol_1_target_quantity * symbol_1_price
        symbol_2_order_value = symbol_2_target_quantity * symbol_2_price

        symbol_1_order_value = Decimal(symbol_1_order_value)
        symbol_2_order_value = Decimal(symbol_2_order_value)

        logging.info(f'{symbol_1} POS size: {symbol_1_target_quantity} | POS value: {symbol_1_order_value}')
        logging.info(f'{symbol_2} POS size: {symbol_2_target_quantity} | POS value: {symbol_2_order_value}')
        
        if (symbol_1_order_value > symbol_alloction_threshold) or (symbol_2_order_value > symbol_alloction_threshold):
            logging.error(f'A Position Value Exceeds Allocation Threshold.')
            continue
        if (symbol_1_order_value < symbol_1_min_notional) or (symbol_2_order_value < symbol_2_min_notional):
            logging.error(f'A Position does not meet MIN_NOTIONAL value.')
            continue

        logging.info(f'\n{positions}')
        if not positions.empty:
            logging.info(f'Pair Positions = TRUE | Initiate Position Management Loop')
            if positions['status'].iloc[-1] == 'NEW' and positions['spread'].iloc[-1] == 'short':
                symbol_1_balance = positions[positions['symbol'] == symbol_1]['orig_qty'].iloc[-1]
                symbol_2_balance = positions[positions['symbol'] == symbol_2]['orig_qty'].iloc[-1]

            elif positions['status'].iloc[-1] == 'NEW' and positions['spread'].iloc[-1] == 'long':
                symbol_1_balance = positions[positions['symbol'] == symbol_1]['orig_qty'].iloc[-1]
                symbol_2_balance = positions[positions['symbol'] == symbol_2]['orig_qty'].iloc[-1]

            else:
                # Handle the case when positions for symbol_1 are not found
                symbol_1_balance = 0 
                symbol_2_balance = 0
            
            logging.info(f'symbol_1 balance: {symbol_1_balance}, symbol_2 balance: {symbol_2_balance}')

            # --------------------
            # Position Management
            # --------------------
            # Check if positions need to be closed
            if positions['status'].iloc[-1] == 'NEW':
                logging.info(f'Position status = NEW')
                # If position is Long Spread
                if df['in_position'].iloc[-1] == 0 and df['in_position'].iloc[-2] == 1:
                    current_spread = 'closed'
                    logging.info('Close Long Spread')
                    logging.info(f'{symbol_1} amount to sell {symbol_1_balance}')
                    logging.info(f'{symbol_2} amount to buy {symbol_2_balance}')

                    try:
                        # Close long spread positions
                        logging.info('Create order to close long spread')
                        order_1 = client.new_order(symbol=symbol_1, side='SELL', type='MARKET', quantity= symbol_1_balance, positionSide= 'LONG')
                        logging.info(f'Order 1 Completed: {order_1}')  # Logging the order details
                        order_2 = client.new_order(symbol=symbol_2, side='BUY', type='MARKET', quantity= symbol_2_balance, positionSide= 'SHORT')
                        logging.info(f'Order 2 Completed: {order_2}')  # Logging the order details

                        order_1_data= {
                            'order_id': order_1['orderId'],
                            'symbol': order_1['symbol'],
                            'pair': f'{symbol_1}-{symbol_2}',
                            'pair_order': 1, # hard code the order pair number
                            'status': order_1['status'],
                            'spread': current_spread,
                            'client_order_id': order_1['clientOrderId'],
                            'price': order_1['price'],
                            'avg_price': order_1['avgPrice'],
                            'orig_qty': order_1['origQty'],
                            'executed_qty': order_1['executedQty'],
                            'cum_qty': order_1['cumQty'],
                            'cum_quote': order_1['cumQuote'],
                            'time_in_force': order_1['timeInForce'],
                            'type': order_1['type'],
                            'reduce_only': order_1['reduceOnly'],
                            'close_position': order_1['closePosition'],
                            'side': order_1['side'],
                            'position_side': order_1['positionSide'],
                            'stop_price': order_1['stopPrice'],
                            'working_type': order_1['workingType'],
                            'price_protect': order_1['priceProtect'],
                            'orig_type': order_1['origType'],
                            'price_match': order_1['priceMatch'],
                            'self_trade_prevention_mode': order_1['selfTradePreventionMode'],
                            'good_till_date': order_1['goodTillDate'],
                            'update_time': pd.to_datetime(order_1['updateTime'], unit='ms')
                        } 
                        order_2_data= {
                            'order_id': order_2['orderId'],
                            'symbol': order_2['symbol'],
                            'pair': f'{symbol_1}-{symbol_2}',
                            'pair_order': 2, # hard code the order pair number
                            'status': order_2['status'],
                            'spread': current_spread,
                            'client_order_id': order_2['clientOrderId'],
                            'price': order_2['price'],
                            'avg_price': order_2['avgPrice'],
                            'orig_qty': order_2['origQty'],
                            'executed_qty': order_2['executedQty'],
                            'cum_qty': order_2['cumQty'],
                            'cum_quote': order_2['cumQuote'],
                            'time_in_force': order_2['timeInForce'],
                            'type': order_2['type'],
                            'reduce_only': order_2['reduceOnly'],
                            'close_position': order_2['closePosition'],
                            'side': order_2['side'],
                            'position_side': order_2['positionSide'],
                            'stop_price': order_2['stopPrice'],
                            'working_type': order_2['workingType'],
                            'price_protect': order_2['priceProtect'],
                            'orig_type': order_2['origType'],
                            'price_match': order_2['priceMatch'],
                            'self_trade_prevention_mode': order_2['selfTradePreventionMode'],
                            'good_till_date': order_2['goodTillDate'],
                            'update_time': pd.to_datetime(order_2['updateTime'], unit='ms')
                        } 

                        insert_statement = text('''
                            INSERT INTO orders (
                                order_id,
                                symbol,
                                pair,
                                pair_order,
                                status,
                                spread,
                                client_order_id,
                                price,
                                avg_price,
                                orig_qty,
                                executed_qty,
                                cum_qty,
                                cum_quote,
                                time_in_force,
                                type,
                                reduce_only,
                                close_position,
                                side,
                                position_side,
                                stop_price,
                                working_type,
                                price_protect,
                                orig_type,
                                price_match,
                                self_trade_prevention_mode,
                                good_till_date,
                                update_time
                            ) 
                            VALUES (
                                :order_id,
                                :symbol,
                                :pair,
                                :pair_order,
                                :status,
                                :spread,
                                :client_order_id,
                                :price,
                                :avg_price,
                                :orig_qty,
                                :executed_qty,
                                :cum_qty,
                                :cum_quote,
                                :time_in_force,
                                :type,
                                :reduce_only,
                                :close_position,
                                :side,
                                :position_side,
                                :stop_price,
                                :working_type,
                                :price_protect,
                                :orig_type,
                                :price_match,
                                :self_trade_prevention_mode,
                                :good_till_date,
                                :update_time
                            )
                        ''')

                        with engine.connect() as connection:
                            connection.execute(insert_statement, parameters= order_1_data)
                            connection.execute(insert_statement, parameters= order_2_data)
                            connection.commit()

                    except ClientError as e:
                        logging.error(f"Error placing orders: {str(e)}")
                        raise

                # If position is Short Spread
                if df['in_position'].iloc[-1] == 0 and df['in_position'].iloc[-2] == -1:
                    logging.info('Close Short Spread')
                    current_spread = 'closed'
                    logging.info(f'{symbol_1} amount to buy {symbol_1_balance}')
                    logging.info(f'{symbol_2} amount to sell {symbol_2_balance}')

                    try:
                        # # Close short spread positions
                        logging.info('Create order to close Short Spread')                   
                        order_1 = client.new_order(symbol=symbol_1, side='BUY', type='MARKET', quantity= symbol_1_balance, positionSide= 'SHORT')
                        logging.info(f'Order 1 Completed: {order_1}')  # Logging the order details
                        order_2 = client.new_order(symbol=symbol_2, side='SELL', type='MARKET', quantity= symbol_2_balance, positionSide= 'LONG')
                        logging.info(f'Order 2 Completed: {order_2}')  # Logging the order details

                        order_1_data= {
                            'order_id': order_1['orderId'],
                            'symbol': order_1['symbol'],
                            'pair': f'{symbol_1}-{symbol_2}',
                            'pair_order': 1, # hard code the order pair number
                            'status': order_1['status'],
                            'spread': current_spread,
                            'client_order_id': order_1['clientOrderId'],
                            'price': order_1['price'],
                            'avg_price': order_1['avgPrice'],
                            'orig_qty': order_1['origQty'],
                            'executed_qty': order_1['executedQty'],
                            'cum_qty': order_1['cumQty'],
                            'cum_quote': order_1['cumQuote'],
                            'time_in_force': order_1['timeInForce'],
                            'type': order_1['type'],
                            'reduce_only': order_1['reduceOnly'],
                            'close_position': order_1['closePosition'],
                            'side': order_1['side'],
                            'position_side': order_1['positionSide'],
                            'stop_price': order_1['stopPrice'],
                            'working_type': order_1['workingType'],
                            'price_protect': order_1['priceProtect'],
                            'orig_type': order_1['origType'],
                            'price_match': order_1['priceMatch'],
                            'self_trade_prevention_mode': order_1['selfTradePreventionMode'],
                            'good_till_date': order_1['goodTillDate'],
                            'update_time': pd.to_datetime(order_1['updateTime'], unit='ms')
                        } 
                        order_2_data= {
                            'order_id': order_2['orderId'],
                            'symbol': order_2['symbol'],
                            'pair': f'{symbol_1}-{symbol_2}',
                            'pair_order': 2, # hard code the order pair number
                            'status': order_2['status'],
                            'spread': current_spread,
                            'client_order_id': order_2['clientOrderId'],
                            'price': order_2['price'],
                            'avg_price': order_2['avgPrice'],
                            'orig_qty': order_2['origQty'],
                            'executed_qty': order_2['executedQty'],
                            'cum_qty': order_2['cumQty'],
                            'cum_quote': order_2['cumQuote'],
                            'time_in_force': order_2['timeInForce'],
                            'type': order_2['type'],
                            'reduce_only': order_2['reduceOnly'],
                            'close_position': order_2['closePosition'],
                            'side': order_2['side'],
                            'position_side': order_2['positionSide'],
                            'stop_price': order_2['stopPrice'],
                            'working_type': order_2['workingType'],
                            'price_protect': order_2['priceProtect'],
                            'orig_type': order_2['origType'],
                            'price_match': order_2['priceMatch'],
                            'self_trade_prevention_mode': order_2['selfTradePreventionMode'],
                            'good_till_date': order_2['goodTillDate'],
                            'update_time': pd.to_datetime(order_2['updateTime'], unit='ms')
                        } 

                        insert_statement = text('''
                            INSERT INTO orders (
                                order_id,
                                symbol,
                                pair,
                                pair_order,
                                status,
                                spread,
                                client_order_id,
                                price,
                                avg_price,
                                orig_qty,
                                executed_qty,
                                cum_qty,
                                cum_quote,
                                time_in_force,
                                type,
                                reduce_only,
                                close_position,
                                side,
                                position_side,
                                stop_price,
                                working_type,
                                price_protect,
                                orig_type,
                                price_match,
                                self_trade_prevention_mode,
                                good_till_date,
                                update_time
                            ) 
                            VALUES (
                                :order_id,
                                :symbol,
                                :pair,
                                :pair_order,
                                :status,
                                :spread,
                                :client_order_id,
                                :price,
                                :avg_price,
                                :orig_qty,
                                :executed_qty,
                                :cum_qty,
                                :cum_quote,
                                :time_in_force,
                                :type,
                                :reduce_only,
                                :close_position,
                                :side,
                                :position_side,
                                :stop_price,
                                :working_type,
                                :price_protect,
                                :orig_type,
                                :price_match,
                                :self_trade_prevention_mode,
                                :good_till_date,
                                :update_time
                            )
                        ''')

                        with engine.connect() as connection:
                            connection.execute(insert_statement, parameters= order_1_data)
                            connection.execute(insert_statement, parameters= order_2_data)
                            connection.commit()

                    except ClientError as e:
                        logging.error(f"Error placing orders: {str(e)}")
                        raise

        logging.info(f'Executing new positions loop...')
        # ---------------
        # Long Spread
        #----------------
        if df['in_position'].iloc[-1] == 1 and df['in_position'].iloc[-2] == 0:
        # if df['in_position'].iloc[-1] == 1:FOR TESTING out of sequence
            current_spread = 'long'
            logging.info(f'Required position = Long Spread')
            logging.info('Placing new long spread orders')

            try:
                # Place orders to open long spread
                logging.info(f'{symbol_1} price: {symbol_1_price} | {symbol_2} price: {symbol_2_price}')

                logging.info(f'Placing BUY order | {symbol_1_target_quantity} {symbol_1}')
                try:
                    order_1 = client.new_order(symbol=symbol_1, side='BUY', type='MARKET', quantity=symbol_1_target_quantity, positionSide='LONG')
                    logging.info(f'Order 1 Completed: {order_1}')  # Logging the order details

                    # Extract specific details from the order response and log them individually
                    order_id = order_1.get('orderId')
                    status = order_1.get('status')
                    
                    if order_id == 0:
                        logging.error(f'Order 1 ID is 0 should not execute order 2')

                    # Log individual order details
                    logging.info(f'Order 1 ID: {order_id}')
                    logging.info(f'Order 1 Status: {status}')
                    # Log other relevant order details similarly

                except ClientError as e:
                    logging.error(f"Error placing order_1: {str(e)}")

                logging.info(f'Placing SELL order | {symbol_2_target_quantity} {symbol_2} ')
                try:
                    order_2 = client.new_order(symbol=symbol_2, side='SELL', type='MARKET', quantity=symbol_2_target_quantity, positionSide='SHORT')
                    logging.info(f'Order 2 Completed: {order_2}')  # Logging the order details

                    # Extract specific details from the order response and log them individually
                    order_id = order_2.get('orderId')
                    status = order_2.get('status')

                    if order_id == 0:
                        logging.error(f'Order 2 ID is 0 Need to reverse Order 1')
                        # TODO reverse order 1 code

                    # Log individual order details
                    logging.info(f'Order 2 ID: {order_id}')
                    logging.info(f'Order 2 Status: {status}')

                except ClientError as e:
                    logging.error(f"Error placing order 2: {str(e)}")

                order_1_data= {
                    'order_id': order_1['orderId'],
                    'symbol': order_1['symbol'],
                    'pair': f'{symbol_1}-{symbol_2}',
                    'pair_order': 1, # hard code the order pair number
                    'status': order_1['status'],
                    'spread': current_spread,
                    'client_order_id': order_1['clientOrderId'],
                    'price': order_1['price'],
                    'avg_price': order_1['avgPrice'],
                    'orig_qty': order_1['origQty'],
                    'executed_qty': order_1['executedQty'],
                    'cum_qty': order_1['cumQty'],
                    'cum_quote': order_1['cumQuote'],
                    'time_in_force': order_1['timeInForce'],
                    'type': order_1['type'],
                    'reduce_only': order_1['reduceOnly'],
                    'close_position': order_1['closePosition'],
                    'side': order_1['side'],
                    'position_side': order_1['positionSide'],
                    'stop_price': order_1['stopPrice'],
                    'working_type': order_1['workingType'],
                    'price_protect': order_1['priceProtect'],
                    'orig_type': order_1['origType'],
                    'price_match': order_1['priceMatch'],
                    'self_trade_prevention_mode': order_1['selfTradePreventionMode'],
                    'good_till_date': order_1['goodTillDate'],
                    'update_time': pd.to_datetime(order_1['updateTime'], unit='ms')
                } 
                order_2_data= {
                    'order_id': order_2['orderId'],
                    'symbol': order_2['symbol'],
                    'pair': f'{symbol_1}-{symbol_2}',
                    'pair_order': 2, # hard code the order pair number
                    'status': order_2['status'],
                    'spread': current_spread,
                    'client_order_id': order_2['clientOrderId'],
                    'price': order_2['price'],
                    'avg_price': order_2['avgPrice'],
                    'orig_qty': order_2['origQty'],
                    'executed_qty': order_2['executedQty'],
                    'cum_qty': order_2['cumQty'],
                    'cum_quote': order_2['cumQuote'],
                    'time_in_force': order_2['timeInForce'],
                    'type': order_2['type'],
                    'reduce_only': order_2['reduceOnly'],
                    'close_position': order_2['closePosition'],
                    'side': order_2['side'],
                    'position_side': order_2['positionSide'],
                    'stop_price': order_2['stopPrice'],
                    'working_type': order_2['workingType'],
                    'price_protect': order_2['priceProtect'],
                    'orig_type': order_2['origType'],
                    'price_match': order_2['priceMatch'],
                    'self_trade_prevention_mode': order_2['selfTradePreventionMode'],
                    'good_till_date': order_2['goodTillDate'],
                    'update_time': pd.to_datetime(order_2['updateTime'], unit='ms')
                } 

                insert_statement = text('''
                    INSERT INTO orders (
                        order_id,
                        symbol,
                        pair,
                        pair_order,
                        status,
                        spread,
                        client_order_id,
                        price,
                        avg_price,
                        orig_qty,
                        executed_qty,
                        cum_qty,
                        cum_quote,
                        time_in_force,
                        type,
                        reduce_only,
                        close_position,
                        side,
                        position_side,
                        stop_price,
                        working_type,
                        price_protect,
                        orig_type,
                        price_match,
                        self_trade_prevention_mode,
                        good_till_date,
                        update_time
                    ) 
                    VALUES (
                        :order_id,
                        :symbol,
                        :pair,
                        :pair_order,
                        :status,
                        :spread,
                        :client_order_id,
                        :price,
                        :avg_price,
                        :orig_qty,
                        :executed_qty,
                        :cum_qty,
                        :cum_quote,
                        :time_in_force,
                        :type,
                        :reduce_only,
                        :close_position,
                        :side,
                        :position_side,
                        :stop_price,
                        :working_type,
                        :price_protect,
                        :orig_type,
                        :price_match,
                        :self_trade_prevention_mode,
                        :good_till_date,
                        :update_time
                    )
                ''')

                with engine.connect() as connection:
                    connection.execute(insert_statement, parameters= order_1_data)
                    connection.execute(insert_statement, parameters= order_2_data)
                    connection.commit()

            except ClientError as e:
                logging.error(f"Error placing orders: {str(e)}")

        # ---------------
        # Short Spread
        #----------------
        if df['in_position'].iloc[-1] == -1 and df['in_position'].iloc[-2] == 0:
        # if df['in_position'].iloc[-1] == -1: FOR TESTING out of sequence
            current_spread = 'short'
            logging.info(f'Required position = Short Spread')
            logging.info('Placing new short spread orders')

            try:
                # Place orders to open short spread
                logging.info(f'{symbol_1} price: {symbol_1_price} | {symbol_2} price: {symbol_2_price}')

                logging.info(f'Placing SELL order | {symbol_1_target_quantity} {symbol_1}')
                try:
                    order_1 = client.new_order(symbol=symbol_1, side='SELL', type='MARKET', quantity= symbol_1_target_quantity, positionSide= 'SHORT')
                    logging.info(f'Order 1 Completed: {order_1}')  # Logging the order details
                    
                    # Extract specific details from the order response and log them individually
                    order_id = order_1.get('orderId')
                    status = order_1.get('status')
                    
                    if order_id == 0:
                        logging.error(f'Order 1 ID is 0 should not execute order 2')

                    # Log individual order details
                    logging.info(f'Order 1 ID: {order_id}')
                    logging.info(f'Order 1 Status: {status}')
                    # Log other relevant order details similarly

                except ClientError as e:
                    logging.error(f"Error placing order_1: {str(e)}")

                logging.info(f'Placing BUY order | {symbol_2_target_quantity} {symbol_2} ')
                try:
                    order_2 = client.new_order(symbol=symbol_2, side='BUY', type='MARKET', quantity= symbol_2_target_quantity, positionSide= 'LONG')
                    logging.info(f'Order 2 Completed: {order_2}')  # Logging the order details
                    
                    # Extract specific details from the order response and log them individually
                    order_id = order_2.get('orderId')
                    status = order_2.get('status')

                    if order_id == 0:
                        logging.error(f'Order 2 ID is 0 Need to reverse Order 1')
                        # TODO reverse order 1 code

                    # Log individual order details
                    logging.info(f'Order 2 ID: {order_id}')
                    logging.info(f'Order 2 Status: {status}')
                except ClientError as e:
                    logging.error(f'Order 2 error: {e}')

                order_1_data= {
                    'order_id': order_1['orderId'],
                    'symbol': order_1['symbol'],
                    'pair': f'{symbol_1}-{symbol_2}',
                    'pair_order': 1, # hard code the order pair number
                    'status': order_1['status'],
                    'spread': current_spread,
                    'client_order_id': order_1['clientOrderId'],
                    'price': order_1['price'],
                    'avg_price': order_1['avgPrice'],
                    'orig_qty': order_1['origQty'],
                    'executed_qty': order_1['executedQty'],
                    'cum_qty': order_1['cumQty'],
                    'cum_quote': order_1['cumQuote'],
                    'time_in_force': order_1['timeInForce'],
                    'type': order_1['type'],
                    'reduce_only': order_1['reduceOnly'],
                    'close_position': order_1['closePosition'],
                    'side': order_1['side'],
                    'position_side': order_1['positionSide'],
                    'stop_price': order_1['stopPrice'],
                    'working_type': order_1['workingType'],
                    'price_protect': order_1['priceProtect'],
                    'orig_type': order_1['origType'],
                    'price_match': order_1['priceMatch'],
                    'self_trade_prevention_mode': order_1['selfTradePreventionMode'],
                    'good_till_date': order_1['goodTillDate'],
                    'update_time': pd.to_datetime(order_1['updateTime'], unit='ms')
                }                
                order_2_data= {
                    'order_id': order_2['orderId'],
                    'symbol': order_2['symbol'],
                    'pair': f'{symbol_1}-{symbol_2}',
                    'pair_order': 2, # hard code the order pair number
                    'status': order_2['status'],
                    'spread': current_spread,
                    'client_order_id': order_2['clientOrderId'],
                    'price': order_2['price'],
                    'avg_price': order_2['avgPrice'],
                    'orig_qty': order_2['origQty'],
                    'executed_qty': order_2['executedQty'],
                    'cum_qty': order_2['cumQty'],
                    'cum_quote': order_2['cumQuote'],
                    'time_in_force': order_2['timeInForce'],
                    'type': order_2['type'],
                    'reduce_only': order_2['reduceOnly'],
                    'close_position': order_2['closePosition'],
                    'side': order_2['side'],
                    'position_side': order_2['positionSide'],
                    'stop_price': order_2['stopPrice'],
                    'working_type': order_2['workingType'],
                    'price_protect': order_2['priceProtect'],
                    'orig_type': order_2['origType'],
                    'price_match': order_2['priceMatch'],
                    'self_trade_prevention_mode': order_2['selfTradePreventionMode'],
                    'good_till_date': order_2['goodTillDate'],
                    'update_time': pd.to_datetime(order_2['updateTime'], unit='ms')
                } 

                insert_statement = text('''
                    INSERT INTO orders (
                        order_id,
                        symbol,
                        pair,
                        pair_order,
                        status,
                        spread,
                        client_order_id,
                        price,
                        avg_price,
                        orig_qty,
                        executed_qty,
                        cum_qty,
                        cum_quote,
                        time_in_force,
                        type,
                        reduce_only,
                        close_position,
                        side,
                        position_side,
                        stop_price,
                        working_type,
                        price_protect,
                        orig_type,
                        price_match,
                        self_trade_prevention_mode,
                        good_till_date,
                        update_time
                    ) 
                    VALUES (
                        :order_id,
                        :symbol,
                        :pair,
                        :pair_order,
                        :status,
                        :spread,
                        :client_order_id,
                        :price,
                        :avg_price,
                        :orig_qty,
                        :executed_qty,
                        :cum_qty,
                        :cum_quote,
                        :time_in_force,
                        :type,
                        :reduce_only,
                        :close_position,
                        :side,
                        :position_side,
                        :stop_price,
                        :working_type,
                        :price_protect,
                        :orig_type,
                        :price_match,
                        :self_trade_prevention_mode,
                        :good_till_date,
                        :update_time
                    )
                ''')

                with engine.connect() as connection:
                    connection.execute(insert_statement, parameters= order_1_data)
                    connection.execute(insert_statement, parameters= order_2_data)
                    connection.commit()

            except ClientError as e:
                logging.error(f"Error placing orders: {str(e)}")
        # else:
        #     logging.info(f'Order value = 0 OR < min_notional value ...')
        logging.info('End of trade executions')
    # # %%
    #     # --------------------
    #     # Pair data collection
    #     # --------------------
    #     logging.info(f'Collecting data for pair: {pair_1}-{pair_2}')

    #     # Calculate the combined positions for the pair
    #     positions = np.array(long_spread) + np.array(short_spread)
    #     positions = pd.DataFrame(positions, columns=(f'{pair_1}', f'{pair_2}'), index = df['date'])
    #     all_positions = pd.concat([all_positions, positions], axis=1)

    #     # Calculate daily returns for both assets
    #     daily_return= df.loc[:, (f'close_{(pair_1).lower()}', f'close_{(pair_2).lower()}')].pct_change()

    #     df[f'dly_rt_{pair_1}'] = daily_return[f'close_{(pair_1).lower()}']
    #     df[f'dly_rt_{pair_2}'] = daily_return[f'close_{(pair_2).lower()}']

    #     # Calculate 'pnl' considering position sizing
    #     pnl = (position_size * np.array(positions.shift()) * np.array(daily_return)).sum(axis=1)

    #     # Calculate transaction fees for rows where 'spread_change' is True
    #     transaction_fees = position_size * transaction_fee_rate
    #     transaction_fees = transaction_fees * spread_change  # Apply the condition to transaction fees

    #     # Subtract transaction fees from pnl for rows with a position change
    #     pnl -= transaction_fees

    #     # Set transaction fees to zero for rows without a position change
    #     df['transaction_fees'] = transaction_fees * spread_change

    #     # Add pnl to the DataFrame
    #     df['pnl'] = pnl
    #     df['cumulative_pnl'] = df['pnl'].cumsum()
    #     df['account_balance'] = initial_account_balance + df['cumulative_pnl']

    #     # Update the account balance with the final balance in the DataFrame
    #     account_balance = df.iloc[-1]['account_balance']

    #     # Add cumulative pnl to the dictionary
    #     all_pairs_pnl_dict[f'{(pair_1).lower()}-{(pair_2).lower()}'] = df[['date','cumulative_pnl']].copy()

    #     # Calculate the Sharpe ratio for the data set (annualized)
    #     if df['pnl'][1:].std() != 0:
    #         sharperatio = np.sqrt(252) * df['pnl'][1:].mean() / df['pnl'][1:].std()
    #     else:
    #         sharperatio = np.nan
    #         logging.info(f'No trade setups for {(pair_1).lower()}-{(pair_2).lower()}')

    #     # Calculate training and test data stats
    #     total_pnl = df['cumulative_pnl'].iloc[-1]

    #     days = df.shape[0]

    #     # Append the results to the list
    #     backtest_stats.append([
    #         f'{(pair_1).lower()}-{(pair_2).lower()}',
    #         hedge_ratio,
    #         spread_mean,
    #         spread_std,
    #         sharperatio,
    #         total_pnl,
    #         days,
    #     ])


    # # ------------------------
    # # Monthly Data Collection 
    # # ------------------------
    # logging.info(f'End of pairs loop, compiling monthly data...')

    # # Create a DataFrame from the results list
    # backtest_stats = pd.DataFrame(backtest_stats, columns=[
    #     'pair',
    #     'hedge_ratio',
    #     'spread_mean',
    #     'spread_std',
    #     'sharperatio',
    #     'total_pnl',
    #     'days',
    # ])

    # all_pairs_dd['cumulative_dd'] = all_pairs_dd.sum(axis=1)

    # # Convert DataFrame to JSON

    # # all_positions_json = all_positions.to_json(orient='records', date_format='iso')
    # # all_positions_json

    # # # Define the SQL INSERT statement
    # # insert_query = text('''
    # #     INSERT INTO positions (date, positions)
    # #     VALUES (:date, :json_data)
    # # ''')

    # # today = datetime.today().date()

    # # data_to_insert = {
    # #     'date': today,
    # #     'json_data': all_positions_json
    # # }

    # # try:
    # #     # Execute the insertion query
    # #     with engine.connect() as connection:
    # #         connection.execute(insert_query, data_to_insert)
    # #         connection.commit()
    # # except SQLAlchemyError as e:
    # #     logging.info(e)


    # # Concatenate DataFrames in the dictionary vertically to create the final DataFrame
    # all_pairs_pnl = pd.concat(all_pairs_pnl_dict.values(), axis=1, keys=all_pairs_pnl_dict.keys())

    # # Convert DataFrame to JSON
    # pnl_json = all_pairs_pnl.to_json(orient='records', date_format='iso')

    # # Define the SQL INSERT statement
    # insert_pnl_query = text('''
    #     INSERT INTO all_pairs_pnl (date, pnl)
    #     VALUES (:date, :json_data)
    # ''')

    # today = datetime.today().date()

    # data_to_insert = {
    #     'date': today,
    #     'json_data': pnl_json
    # }

    # try:
    #     # Execute the insertion query
    #     with engine.connect() as connection:
    #         connection.execute(insert_pnl_query, data_to_insert)
    #         connection.commit()
    # except SQLAlchemyError as e:
    #     logging.error(e)

    # # Create a DataFrame from the results list and sort by best sharpe ratios
    # current_results_df = backtest_stats.sort_values('sharperatio', ascending=False)
    # logging.info(f'{current_results_df}')


    # '''TODO The code below should be used in an explorer notebook to explore the results and visualise'''
    # # # %%
    # # all_pairs_pnl['cumulative_pnl'] = all_pairs_pnl.sum(axis=1)
    # # all_pairs_pnl.ffill(inplace=True)
    # # all_pairs_pnl['cumulative_pnl'] = all_pairs_pnl.sum(axis=1)

    # # # %%
    # # all_pairs_pnl_adj = all_pairs_pnl.copy()
    # # all_pairs_pnl_adj = all_pairs_pnl_adj + initial_account_balance

    # # # %%
    # # first_non_zero_value = abs(all_pairs_pnl_adj['cumulative_pnl'].loc[all_pairs_pnl_adj['cumulative_pnl'] != 0].iloc[0])
    # # norm_all_pairs = all_pairs_pnl_adj['cumulative_pnl'] / first_non_zero_value

    # # %%
    # # # Load BTC data
    # # btc_query = text('''
    # #     SELECT 
    # #         ap.open_time AS timestamp,
    # #         a.symbol,
    # #         ap.volume,
    # #         ap.open AS vwap,
    # #         ap.open,
    # #         ap.close,
    # #         ap.high,
    # #         ap.low,
    # #         ap.volume AS trades
    # #     FROM asset_price AS ap
    # #     INNER JOIN asset AS a
    # #     ON ap.asset_id = a.id
    # #     WHERE a.symbol IN ('BTCUSDT')
    # #     AND ap.open_time BETWEEN :start_date AND :end_date 
    # #     ORDER BY ap.open_time;          
    # # ''')

    # # btc_data = pd.read_sql(btc_query, engine, params={'start_date': backtest_dataset_start, 'end_date': backtest_dataset_end})
    # # btc_data
    # # norm_btc = btc_data['close'] / btc_data['close'].iloc[0]
    # # norm_btc

    # # # %%

    # # # # Adjust the data to start at y = 1
    # # # norm_all_pairs = norm_all_pairs[test_set.index[0:]] / norm_all_pairs[train_set.index[-1]] # Normalize to the initial value
    # # # norm_btc = norm_btc[train_set.index[0:]] / norm_btc[train_set.index[-1]]  # Normalize to the initial value

    # # # Create a figure and axis
    # # plt.figure(figsize=(20, 5))
    # # ax = plt.gca()

    # # # Plot both normalized DataFrames on the same chart
    # # ax.plot(norm_all_pairs, label='Top Pairs Cumulative', color='b')
    # # ax.plot(norm_btc, label='BTCUSD', color='r')

    # # # Set labels and legend
    # # plt.title(f'Normalized Strategy P&L and BTCUSD at {(position_size / initial_account_balance * 100)}% capital deployed per trade')
    # # plt.xlabel('Date')
    # # plt.ylabel('Normalized Value')
    # # plt.legend()
    # # plt.grid()
    # # plt.show()

    # # # TODO fix the all_pairs_pnl df so that the percentage return is calculated from starting at 5000

    # # plt.figure(figsize=(20, 3))

    # # max_dd, dd_plot = calculate_dd(all_pairs_pnl)
    # # plt.plot(dd_plot['cumulative_dd'])

    # # plt.show()