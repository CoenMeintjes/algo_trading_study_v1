import datetime
from loguru import logger
import azure.functions as func
from data_fetch import data_fetch
from execution import execution_model

app = func.FunctionApp()

logger.add('logs/0_function_app.log', rotation= '5 MB')

@app.function_name(name="mytimer")

# 1 sec after midnight = '1 0 * * * *'
# every 5 sec = '*/5 * * * * *'

@app.schedule(schedule='0 */5 * * * *', arg_name="mytimer", run_on_startup=False,
              use_monitor=False) 

def test_function(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().isoformat()

    if mytimer.past_due:
        logger.info('The timer is past due!')

    logger.info(f'Python timer trigger function ran at {utc_timestamp}')
    data_fetch()
    logger.info(f'Data fetch finished starting execution model...')
    execution_model()
    logger.info(f'Execution model done.')

