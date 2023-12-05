from loguru import logger
import azure.functions as func
from datetime import datetime, timedelta
from data_fetch import data_fetch
from execution import execution_model
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
from azure_utils import get_secret
import os

# Instantiate the app
app = func.FunctionApp()

### DEFINE FUNCTION
# {second} {minute} {hour} {day} {month} {day-of-week}
# function will trigger at 1 min past midnight utc every day = '0 1 0 * * *'
@app.timer_trigger(
    schedule='0 1 0 * * *', 
    arg_name="mytimer",
    run_on_startup=False,
    use_monitor=False
) 
def algo_v1(mytimer: func.TimerRequest) -> None:

    ### ENVIRONMENT VARIABLES
    try:
        connection_string = os.getenv('SQLConnectionString')
        sg_api = os.getenv('SendGridString')
        binance_api = get_secret('binance-api-1')
        binance_secret = get_secret('binance-secret')  

    except Exception as e:
        logger.error(f'Error fetching keys: {e}')

    today = (datetime.now()).date()
    yesterday = today - timedelta(days= 1)

    ### RUN DATA FETCH    
    try: 
        logger.info(f'Fetching data up to end of {yesterday}')
        data_fetch(
            start= yesterday,
            end= yesterday,
            connection_string= connection_string
        )
        logger.info(f'Data fetch successful.')

    except Exception as e:
        logger.error(f'Error running the data_fetch | {e}')

    ### RUN THE EXECUTION MODEL
    try:
        logger.info(f'Running execution_model script...')
        execution_model(
            binance_api = binance_api,
            binance_secret= binance_secret,
            connection_string= connection_string,
        )
        logger.info(f'Execution model successful.')
    except Exception as e:
        logger.error(f'Error running the execution model | {e}')

    ### SEND EMAIL NOTIFICATION 
    logger.info(f'Sending notification')
    message = Mail(
        from_email='chmeintjes@gmail.com',
        to_emails='chmeintjes@gmail.com',
        subject='Function Run Test',
        html_content= 'Function has successfully run.'
    )
    try:
        sg = SendGridAPIClient(sg_api)
        response = sg.send(message)
        logger.info(f'sendgrid response status code | {response.status_code}')

    except Exception as e:
        logger.error(e)

    logger.info(f'Function run complete.')

