import logging
import polars as pl
import azure.functions as func


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    storage_options={'account_name': 'gen2poc', 'anon': False}
    try:
        df = pl.read_parquet('az://mycontainer/acl-check/green_tripdata_2022-02.parquet', storage_options=storage_options)
        logging.debug(df.head(1))
        firstrow = df.head(1)
        return func.HttpResponse(f"The columns of dataframe are {firstrow.columns}. This HTTP triggered function executed successfully.")
    except Exception as e:
        logging.exception(f"Something went wrong {e}")
        return func.HttpResponse(f"Something went wrong while calling the api - {e}",status_code=500)

