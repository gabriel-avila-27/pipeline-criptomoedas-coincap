import os

from google.cloud.bigquery import Client
from google.oauth2 import service_account
from dotenv import load_dotenv



# Carregando variáveis de ambiente
load_dotenv(dotenv_path='.env')

#Váriaveis de ambiente
BQ_PROJECT_ID = os.getenv("BQ_PROJECT_ID")
GCP_CREDENTIALS = os.getenv("GCP_CREDENTIALS")
BQ_BRZ_DATASET_ID = os.getenv("BQ_BRZ_DATASET_ID")
BQ_SVR_DATASET_ID = os.getenv("BQ_SVR_DATASET_ID")
BQ_GLD_DATASET_ID = os.getenv("BQ_GLD_DATASET_ID")


def big_query_query_job(sql_statement):
    auth = service_account.Credentials.from_service_account_file(
        filename=GCP_CREDENTIALS, scopes=["https://www.googleapis.com/auth/bigquery"]

    )

    # Iniciando um Clinte para interagir com o Big Query
    bq_client = Client(credentials=auth)

    for sql_statement in sql_statements:
        try:
            query_job = bq_client.query(sql_statement)
            # Aguarda o big query terminar de executar a tarefa antes de retornar a execução do script python
            query_job.result()
            print("Big Query Job Excecutado com sucesso!")

        except Exception as e:
            print("Erro! Não foi possível executar o sql statement!")
            print(f"Error = {e}")

    return None


sql_statements = [
    f"""
    CREATE TABLE IF NOT EXISTS `{BQ_PROJECT_ID}.{BQ_SVR_DATASET_ID}.assets_history`(
    price_usd FLOAT64,
    time_in_milliseconds INT64,
    date DATE,
    id STRING,
    extracted_at TIMESTAMP)
    """,
    f"""
    MERGE INTO `{BQ_PROJECT_ID}.{BQ_SVR_DATASET_ID}.assets_history` AS T
    USING (SELECT 
            ROUND(CAST(priceUsd AS FLOAT64), 2) AS price_usd,	
            time AS time_in_milliseconds,	
            DATE(date) AS date,			
            id,	
            extracted_at 
            FROM `{BQ_PROJECT_ID}.{BQ_BRZ_DATASET_ID}.assets_history` 
            WHERE extracted_at NOT IN(SELECT DISTINCT(extracted_at) FROM `{BQ_PROJECT_ID}.{BQ_SVR_DATASET_ID}.assets_history`)) AS S
    ON T.id = S.id AND T.date = S.date
    --WHEN MATCHED THEN
    --UPDATE SET
    -- Caso necessário, é possível atualizar os registros existentes com um novo valor para a determinada coluna
    WHEN NOT MATCHED THEN
    INSERT (price_usd, time_in_milliseconds, date, id, extracted_at) 
    VALUES (S.price_usd, S.time_in_milliseconds, S.date, S.id, S.extracted_at)
    """,
    f"""CREATE TABLE IF NOT EXISTS `{BQ_PROJECT_ID}.{BQ_GLD_DATASET_ID}.assets_history` (
        id STRING,
        symbol STRING,
        price_usd FLOAT64,
        price_change_last_24_hrs FLOAT64,
        price_change_percentage_last_24_hrs FLOAT64,
        time_in_milliseconds INT64,
        date DATE,
        extracted_at TIMESTAMP)
    """,
    f""" 
    MERGE INTO `{BQ_PROJECT_ID}.{BQ_GLD_DATASET_ID}.assets_history` AS T
    USING(
    SELECT
        id,
        CASE 
          WHEN id = 'aave' THEN 'AAVE'
          WHEN id = 'bitcoin' THEN 'BTC'
          WHEN id = 'chainlink' THEN 'LINK'
          WHEN id = 'ethereum' THEN 'ETH'
          WHEN id = 'solana' THEN 'SOL'
          WHEN id = 'xrp' THEN 'XRP'
          WHEN id = 'cardano' THEN 'ADA'
          WHEN id = 'dogecoin' THEN 'DOGE'
          WHEN id = 'tron' THEN 'TRX'
          WHEN id = 'monero' THEN 'XMR'
          ELSE 'UNKNOWN'
        END AS symbol,
        price_usd,
        ROUND(price_usd - COALESCE(LAG(price_usd) OVER (PARTITION BY id ORDER BY date), price_usd), 2) AS price_change_last_24_hrs,
        ROUND(IFNULL(((price_usd - LAG(price_usd) OVER (PARTITION BY id ORDER BY date)) / LAG(price_usd) OVER (PARTITION BY id ORDER BY date)), 0) * 100, 3) AS price_change_percentage_last_24_hrs,
        time_in_milliseconds,
        date,
        extracted_at
    FROM `{BQ_PROJECT_ID}.{BQ_SVR_DATASET_ID}.assets_history` 
    WHERE extracted_at NOT IN (SELECT DISTINCT(extracted_at) FROM `{BQ_PROJECT_ID}.{BQ_GLD_DATASET_ID}.assets_history`)) AS S
    ON T.id = S.id AND T.date = S.date
    --WHEN MATCHED THEN
    --UPDATE SET
    -- Caso necessário, é possível atualizar os registros existentes com um novo valor para a determinada coluna
    WHEN NOT MATCHED THEN
    INSERT (id, symbol, price_usd, price_change_last_24_hrs, price_change_percentage_last_24_hrs, time_in_milliseconds, date, extracted_at) 
    VALUES (S.id, S.symbol, S.price_usd, S.price_change_last_24_hrs, S.price_change_percentage_last_24_hrs, S.time_in_milliseconds, S.date, S.extracted_at)
   """
    ]


if __name__ == "__main__":
    big_query_query_job(sql_statement= sql_statements)