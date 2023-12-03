# %%
from loguru import logger
import azure.functions as func
from datetime import datetime, timedelta
from data_fetch import data_fetch
from execution import execution_model
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

credential = DefaultAzureCredential()

logger.add('logs/0_function_app.log', rotation= '5 MB')

# %%
app = func.FunctionApp()

logger.add('logs/0_function_app.log', rotation= '5 MB')

@app.function_name(name="mytimer")

# function will trigger at 1 min past midnight utc every day
@app.schedule(schedule='0 1 0 * * *', arg_name="mytimer", run_on_startup=False,
              use_monitor=False) 

def test_function(mytimer: func.TimerRequest) -> None:
    today = (datetime.now()).date()
    yesterday = today - timedelta(days= 1)

    if mytimer.past_due:
        logger.error('The timer is past due!')
    
    # Run the data fetch
    logger.info(f'Fetching data up to end of {yesterday}')
    
    data_fetch(select_database='production', start=yesterday, end=yesterday)

    # Run the execution_model
    logger.info(f'Running execution_model script...')

    execution_model(select_database='production')

    logger.info(f'Function run complete.')

