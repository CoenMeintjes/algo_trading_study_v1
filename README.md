# README for Trading Algorithm Backtest Script

## Overview

This script backtests a trading strategy by performing cointegration and ADF tests on pairs of assets. The results are stored in a local PostgreSQL database and then transferred to an Azure database.

## Requirements

- Python 3.x
- Pandas
- Statsmodels
- SQLAlchemy
- Scikit-learn
- Loguru
- Python-dotenv

## Setup

1. Install the required libraries:

```bash
pip install pandas statsmodels sqlalchemy scikit-learn loguru python-dotenv
```

2. Create a `.env` file with the following environment variables:

```
DB_USER=your_db_user
DB_PASSWORD=your_db_password
DB_HOST=your_db_host
DB_PORT=your_db_port
DB_NAME_FUT=your_local_db_name
```

3. Create a `logs` directory to store log files.

## Usage

Run the script locally once a month to populate the Azure database with the backtested results.

## Script Details

1. **Data Preparation:**
    - Defines the date ranges for data extraction.
    - Connects to the local PostgreSQL database.
    - Fetches asset price data and preprocesses it for cointegration testing.

2. **Cointegration Testing:**
    - Tests pairs of assets for cointegration using the Engle-Granger method.
    - Stores the results in the `coint_test_results` table in the local database.
    - Logs the process and handles errors.

3. **ADF Testing:**
    - Performs the Augmented Dickey-Fuller (ADF) test on the spread of cointegrated pairs.
    - Stores the results in the `adf_test_results` table in the local database.
    - Logs the process and handles errors.

4. **Populating Trading Pairs:**
    - Selects top pairs based on ADF test results.
    - Stores the top pairs in the `trading_pairs` table in the local database.

5. **Populating Azure Database:**
    - Transfers data from the local database to the Azure database.
    - Handles errors and logs the process.

## TODO

- Improve the solution to reduce runtime (currently > 10 minutes).
- Address issues with running the script directly on the cloud database.
- Implement error handling for cases where there is no data for the selected date range.
- Review the logic of subtracting 20 days from the start date for training set preparation.

## Known Issues

- Running the script directly on the cloud database may cause issues due to differences in sequence between local and cloud databases.
- The script might need further optimization to handle large datasets efficiently.

## Contribution

Feel free to contribute to the project by creating pull requests or reporting issues.

## License

This project is licensed under the MIT License.