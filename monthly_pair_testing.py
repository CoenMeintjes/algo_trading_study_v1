'''Currently this script will not be able to run as a function as it will take too long > 10min limit
    Idea is to run this locally once a month and populate the azure database until I can improve the solution'''

# %% 
import pandas as pd
from statsmodels.tsa.stattools import coint
from statsmodels.api import OLS
from statsmodels.tsa.stattools import adfuller
from itertools import combinations
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from datetime import timedelta, datetime
from sklearn.preprocessing import MinMaxScaler
from loguru import logger
from dotenv import load_dotenv
import os

load_dotenv()
# %%
logger.add('logs/2_pair_testing.log', rotation= '5 MB')

# Assuming df is your DataFrame with columns 'price' and 'volume'
scaler = MinMaxScaler()

start_date = datetime(2023, 10, 25)
end_date = datetime(2023, 11, 25)

date_ranges = []

current_date = start_date
while current_date < end_date:
    next_month = current_date.replace(day=25)  # Set to 25th of current month
    next_month = next_month.replace(month=next_month.month + 1 if next_month.month < 12 else 1,
                                    year=next_month.year + 1 if next_month.month == 12 else next_month.year)
    if next_month > end_date:
        next_month = end_date

    date_ranges.append((current_date, next_month))
    current_date = next_month

# --------------
# SANDBOX DB
# --------------
engine = create_engine(f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME_FUT')}")
# --------------
# PRODUCTION DB
# --------------
prod_engine = create_engine(f"postgresql://{os.getenv('PG_USERNAME')}:{os.getenv('PG_PASSWORD')}@{os.getenv('PG_HOST')}:{os.getenv('PG_PORT')}/{os.getenv('PG_DATABASE')}")
# --------------

query = '''
    SELECT ap.asset_id, a.symbol, ap.open_time, ap.open, ap.high, ap.low, ap.close, ap.volume
    FROM asset_price AS ap
    INNER JOIN asset AS a
    ON ap.asset_id = a.id
'''
df = pd.read_sql(query, engine)

df1 = df.copy()
df1.rename(columns={'open_time': 'timestamp'}, inplace=True)
df1.columns = ['asset_id', 'symbol', 'timestamp', 'open', 'high', 'low', 'close', 'volume']
df1 = df1.sort_values('timestamp')

# -----
'''TODO NEED TO ERROR HANDLE FOR A CASE WHERE THERE IS NO DATA FOR THE SELECTED DATE RANGE'''

# %%
for start, end in date_ranges:
    start_date = start
    end_date = end

    '''TODO don't know why I am subtracting 21 days here I don't think this is right BUT going with it and can adjust after deploying v1'''
    trainset_start = pd.to_datetime(start_date) - timedelta(days = 20)
    trainset_end = pd.to_datetime(end_date)
    logger.info(f'Date range: {trainset_start} --> {trainset_end}')

    # Fetch the asset_price data for the specified date range to be used for test
    query = text('''
        SELECT ap.asset_id, a.symbol, ap.open_time, ap.open, ap.high, ap.low, ap.close, ap.volume
        FROM asset_price AS ap
        INNER JOIN asset AS a
        ON ap.asset_id = a.id
        WHERE ap.open_time >= :dataset_start AND ap.open_time <= :dataset_end
    ''')
    df2 = pd.read_sql(query, engine, params={'dataset_start': trainset_start, 'dataset_end': trainset_end})

    # structure the df to be used 
    df3 = df2.copy()
    df3.rename(columns={'open_time': 'timestamp'}, inplace=True)
    df3.columns = ['asset_id', 'symbol', 'timestamp', 'open', 'high', 'low', 'close', 'volume']
    df3 = df3.sort_values('timestamp')

    # Group the data by day and count the timestamps for each day
    daily_counts = df3['timestamp'].value_counts().sort_index()

    # First filter out symbols with too few datapoints
    days_by_symbol = df3.value_counts('symbol')
    logger.info(f'{days_by_symbol.describe()}')

    # Filtering to min 30 days
    mask = days_by_symbol >= 30
    df4 = df3[df3['symbol'].isin(mask.index[mask])]

    # Create a value traded column in the df
    df4.loc[:, 'value_traded'] = df4['close'] * df4['volume']
    
    # Normalise the norm_value_traded column for better comparison
    df4.loc[:, 'norm_value_traded'] = scaler.fit_transform(df4['value_traded'].values.reshape(-1, 1))

    # need to first aggr by symbol then filter out all low value symbols
    value_traded = df4.groupby('symbol')['norm_value_traded'].sum().reset_index()
    quantile_75 = value_traded['norm_value_traded'].quantile(.75) 
    symbol_value = value_traded[value_traded['norm_value_traded'] > quantile_75] 

    # Extract the unique symbols
    symbols = symbol_value['symbol'].unique()
    
    # Create a new filtered dataframe with only the symbols that pass all the filters
    df5 = df4.copy()
    df5 = df5[df5['symbol'].isin(symbols)]
    df5 = df5.sort_values(by=['timestamp'])
    
    # Group the data by day and count the timestamps for each day
    daily_counts = df5['timestamp'].value_counts().sort_index()

    # Number of days in data set 
    no_of_days = df5['timestamp'].max() - df5['timestamp'].min()
    no_of_days = round((no_of_days.total_seconds() / 86400),)

    # Prepped data to use for training
    training_data = df5.copy()

    # Check if existing tests
    query_2 = text('''
        SELECT *
        FROM coint_test_results
        WHERE trainset_start = :trainset_start
                AND trainset_end = :trainset_end
    ''')

    existing_results = pd.read_sql(query_2, engine, params={'trainset_start': trainset_start, 'trainset_end': trainset_end})

    if existing_results.empty:
        logger.info('No test results yet.')
    else:
        tested_symbols = existing_results['pair'].tolist()
        logger.info(f'Number of existing tests: {len(tested_symbols)}')

    # Generate all possible pairs of symbols
    symbol_pairs = list(combinations(symbols, 2))
    logger.info(f'Number of possible pair combinations: {len(symbol_pairs)}')

    # Get the current date and time
    test_date = datetime.now()
    # If you only want the date part (without the time), you can use date()
    test_date = test_date.date()

    # SQL insert statement
    insert_query = text('''
        INSERT INTO coint_test_results (pair, coint_test_stat, p_value, symbol_1_id, symbol_2_id, symbol_1, symbol_2, trainset_start, trainset_end, test_date)
        VALUES (:pair, :coint_t, :pvalue, :symbol_1_id, :symbol_2_id, :symbol_1, :symbol_2, :trainset_start, :trainset_end, :test_date)
    ''')

    # symbol_1_id_query = text('SELECT id FROM asset WHERE symbol = :symbol_1')
    # symbol_2_id_query = text('SELECT id FROM asset WHERE symbol = :symbol_2')

    # # TODO change coint_test_results_ml PK to id pairs and see if this speeds up the query. maybe put date first?
    # record_query = text('''
    #                     SELECT 1 FROM coint_test_results
    #                     WHERE symbol_1_id = :symbol_1_id 
    #                         AND symbol_2_id = :symbol_2_id
    #                         AND trainset_start = :trainset_start
    #                         AND trainset_end = :trainset_end
    #                 ''')
    # Consolidated query to get IDs and check record existence
    combined_query = text('''
        SELECT a1.id AS symbol_1_id, a2.id AS symbol_2_id
        FROM asset AS a1
        JOIN asset AS a2 ON a1.symbol = :symbol_1 AND a2.symbol = :symbol_2
        WHERE EXISTS (
            SELECT 1 FROM coint_test_results
            WHERE symbol_1_id = a1.id
                AND symbol_2_id = a2.id
                AND trainset_start = :trainset_start
                AND trainset_end = :trainset_end
        )
    ''')
    # Loop through pairs of symbols for testing
    for symbol_1, symbol_2 in symbol_pairs:
        pair_name = f'{symbol_1}-{symbol_2}'

        # # Execute the queries to get symbol_1_id and symbol_2_id
        # with engine.connect() as connection:
        #     symbol_1_id_result = connection.execute(symbol_1_id_query, {'symbol_1': symbol_1}).fetchone()
        #     symbol_2_id_result = connection.execute(symbol_2_id_query, {'symbol_2': symbol_2}).fetchone()
        #     symbol_1_id = symbol_1_id_result[0]
        #     symbol_2_id = symbol_2_id_result[0]
        #     primary_key_result = connection.execute(record_query, {'symbol_1_id': symbol_1_id, 'symbol_2_id': symbol_2_id, 'trainset_start': trainset_start, 'trainset_end': trainset_end}).fetchone()
        # Execute the consolidated query to get IDs and check record existence
        commit_count = 0
        with engine.connect() as connection:
            combined_result = connection.execute(
                combined_query,
                {
                    'symbol_1': symbol_1,
                    'symbol_2': symbol_2,
                    'trainset_start': trainset_start,
                    'trainset_end': trainset_end
                }
            ).fetchone()

            # TODO check for record is very slow. I need to pull in the whole database and thne use pandas to filter out I think
            if not combined_result:
                symbol_1_id = combined_result[0]
                symbol_2_id = combined_result[1]
        # if primary_key_result is None:
                logger.info(f'Cointegration testing new pair {pair_name}...')
                # Extract the ID values from the query results
                # symbol_1_id = symbol_1_id_result[0] 
                # symbol_2_id = symbol_2_id_result[0]
                
                # Select rows for the two symbols
                df_symbol_1 = training_data[training_data['symbol'] == symbol_1]
                df_symbol_2 = training_data[training_data['symbol'] == symbol_2]

                # Trim the symbol with more data to match the length of the symbol with less data
                min_length = min(len(symbol_1), len(symbol_2))
                df_symbol_1 = df_symbol_1.iloc[:min_length]
                df_symbol_2 = df_symbol_2.iloc[:min_length]
                try:
                    # Perform cointegration test for the pair of symbols using their 'close' prices
                    coint_t, pvalue, crit_value = coint(df_symbol_1['close'], df_symbol_2['close'])

                    data_to_insert = {
                        'pair': pair_name,
                        'coint_t': coint_t,
                        'pvalue': pvalue,
                        'symbol_1_id': symbol_1_id,
                        'symbol_1': symbol_1,
                        'symbol_2_id': symbol_2_id,
                        'symbol_2': symbol_2,
                        'trainset_start': trainset_start,
                        'trainset_end': trainset_end,
                        'test_date': test_date,
                    }

                    # Execute the insertion query
                    connection.execute(insert_query, data_to_insert)
                    
                    commit_count += 1
                    if commit_count == 10:
                        logger.info(f'{commit_count} assets committed')
                        connection.commit()  # Commit the transaction
                        commit_count = 0

                except Exception as e:
                    logger.error(f"Error for pair {pair_name}: {e}")

            else:
                logger.info('Record exists. Skipping.')
      
    test_results_query = text('''
        SELECT pair, coint_test_stat, p_value, symbol_1, symbol_2, trainset_start, trainset_end, test_date
        FROM coint_test_results
        WHERE p_value < 0.05 AND trainset_start = :trainset_start
        ORDER BY p_value ASC;
    ''')

    best_results = pd.read_sql(test_results_query, engine, params={'trainset_start': trainset_start})

    viable_symbols = pd.concat([best_results['symbol_1'], best_results['symbol_2']]).unique()

    # Filter the DataFrame based on viable_symbols
    test_pairs_data = training_data[training_data['symbol'].isin(viable_symbols)]
    
    # Get the current date and time
    test_date = datetime.now()
    # If you only want the date part (without the time), you can use date()
    test_date = test_date.date()

    record_query = text('''
                        SELECT 1 FROM adf_test_results 
                        WHERE symbol_1_id = :symbol_1_id 
                            AND symbol_2_id = :symbol_2_id
                            AND trainset_start = :trainset_start
                            AND trainset_end = :trainset_end
                    ''')

    symbol_1_id_query = text('SELECT id FROM asset WHERE symbol = :symbol_1')
    symbol_2_id_query = text('SELECT id FROM asset WHERE symbol = :symbol_2')

    # SQL insert statement
    adf_insert_query = text('''
        INSERT INTO adf_test_results (pair, adf_test_stat, p_value, stationary, symbol_1_id, symbol_1, symbol_2_id, symbol_2, trainset_start, trainset_end, test_date)
        VALUES (:pair, :adf_t, :pvalue, :stationary, :symbol_1_id, :symbol_1, :symbol_2_id, :symbol_2, :trainset_start, :trainset_end, :test_date)
    ''')

    # Loop through each pair and perform the ADF test
    for index, row in best_results.iterrows():
        symbol_1 = row['symbol_1']
        symbol_2 = row['symbol_2']
        pair_name = f'{symbol_1}-{symbol_2}'

        # Execute the queries to get symbol_1_id and symbol_2_id
        with engine.connect() as connection:
            symbol_1_id_result = connection.execute(symbol_1_id_query, {'symbol_1': symbol_1}).fetchone()
            symbol_2_id_result = connection.execute(symbol_2_id_query, {'symbol_2': symbol_2}).fetchone()
            symbol_1_id = symbol_1_id_result[0]
            symbol_2_id = symbol_2_id_result[0]
            primary_key_result = connection.execute(record_query, {'symbol_1_id': symbol_1_id, 'symbol_2_id': symbol_2_id, 'trainset_start': trainset_start, 'trainset_end': trainset_end}).fetchone()

        if primary_key_result is None:
            logger.info(f'ADF testing new pair {pair_name}...')
            # Extract the ID values from the query results
        
            try:
                # query the db for pair json data
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
                            AND ap_symbol_1.open_time BETWEEN :trainset_start AND :trainset_end
                    ORDER BY
                        ap_symbol_1.open_time;
                ''')

                pair_data = pd.read_sql_query(pair_data_query, engine, params={'symbol_1': symbol_1, 'symbol_2': symbol_2, 'trainset_start': trainset_start, 'trainset_end': trainset_end})

                # Perform cointegration and calculate the spread as you've done
                model = OLS(pair_data[f'close_{(symbol_1).lower()}'], pair_data[f'close_{(symbol_2).lower()}'])
                results = model.fit()
                hedge_ratio = results.params.iloc[0] 
                spread = pair_data[f'close_{(symbol_1).lower()}'] - hedge_ratio * pair_data[f'close_{(symbol_2).lower()}']
                
                # Perform ADF test on the spread
                adf_test_result = adfuller(spread)
                adf_statistic = adf_test_result[0]
                pvalue = adf_test_result[1]
                
                # Determine stationarity based on p-value
                stationary = bool(pvalue <= 0.05)

                data_to_insert = {
                    'pair': pair_name,
                    'adf_t': adf_statistic,
                    'pvalue': pvalue,
                    'stationary': stationary,
                    'symbol_1_id': symbol_1_id,
                    'symbol_1': symbol_1,
                    'symbol_2_id': symbol_2_id,
                    'symbol_2': symbol_2,
                    'test_date': test_date,
                    'trainset_start': trainset_start,
                    'trainset_end': trainset_end,
                }

                # Execute the insertion query
                with engine.connect() as connection:
                    connection.execute(adf_insert_query, data_to_insert)
                    connection.commit()
            
            except Exception as e:
                logger.error(f"An error occurred for pair {symbol_1} - {symbol_2}: {str(e)}. No pair_data in db.")
                continue
        else:
            logger.info('Record exists. Skipping.')

    # ----------------------------------------
    # Populate trading pairs db with top pairs
    # ----------------------------------------
    top_pairs_query = text('''
        SELECT *
        FROM adf_test_results
        WHERE stationary = TRUE
        ORDER BY adf_test_stat
        LIMIT 25;
    ''')

    top_pairs = pd.read_sql_query(top_pairs_query, engine)

    symbol_1_id_query = text('SELECT id FROM asset WHERE symbol = :symbol_1')
    symbol_2_id_query = text('SELECT id FROM asset WHERE symbol = :symbol_2')

    for i, row in top_pairs.iterrows():

        symbol_1 = row['symbol_1']
        symbol_2 = row['symbol_2']

        record_query = text('''
                            SELECT 1 FROM trading_pairs 
                            WHERE symbol_1_id = :symbol_1_id 
                                AND symbol_2_id = :symbol_2_id
                                AND trainset_start = :trainset_start
                                AND trainset_end = :trainset_end
                        ''')

        with engine.connect() as connection:
            symbol_1_id_result = connection.execute(symbol_1_id_query, {'symbol_1': symbol_1}).fetchone()
            symbol_2_id_result = connection.execute(symbol_2_id_query, {'symbol_2': symbol_2}).fetchone()
            symbol_1_id = symbol_1_id_result[0] 
            symbol_2_id = symbol_2_id_result[0]
            primary_key_result = connection.execute(record_query, {'symbol_1_id': symbol_1_id, 'symbol_2_id': symbol_2_id, 'trainset_start': trainset_start, 'trainset_end': trainset_end}).fetchone()

        if primary_key_result is None:
            
            insert_statement = text('''
                INSERT INTO trading_pairs (symbol_1_id, symbol_2_id, trainset_start, trainset_end, test_date)
                VALUES (:symbol_1_id, :symbol_2_id, :trainset_start, :trainset_end, :test_date)                         
            ''')

            data_to_insert = {
                'symbol_1_id': symbol_1_id, 
                'symbol_2_id': symbol_2_id,
                'test_date': test_date,
                'trainset_start': trainset_start,
                'trainset_end': trainset_end,
                'test_date': test_date
            }

            with engine.connect() as connection:
                connection.execute(insert_statement, data_to_insert)
                connection.commit()

        else:
            logger.info('Record exists. Skipping.')


# %%
# ----------------------
# Populate Azure DB
# ----------------------

# Query data from local databases
coint_query = text('''
    SELECT * FROM coint_test_results;
''')

adf_query = text('''
    SELECT * FROM adf_test_results;
''')

trading_pairs_query = text('''
    SELECT * FROM trading_pairs;
''')

# Create dfs for each table
coint_data = pd.read_sql(con= engine, sql=coint_query)
logger.info(f'No. of rows in coint_test_results | {len(coint_data)}')

adf_data = pd.read_sql(con= engine, sql=adf_query)
logger.info(f'No. of rows in adf_test_results | {len(adf_data)}')

trading_pairs_data = pd.read_sql(con= engine, sql=trading_pairs_query)
logger.info(f'No. of rows in trading_pairs | {len(trading_pairs_data)}')

# insert data into production database table
# Define the tables and corresponding DataFrames
tables_dataframes = [
    ('coint_test_results', coint_data),
    ('adf_test_results', adf_data),
    ('trading_pairs', trading_pairs_data)
]

for table_name, dataframe in tables_dataframes:
    try:
        dataframe.to_sql(table_name, prod_engine, if_exists='append', index=False, method='multi')
    except SQLAlchemyError as e:
        logger.error(f'Error inserting data into table {table_name}: {e}')
        continue

# %%
