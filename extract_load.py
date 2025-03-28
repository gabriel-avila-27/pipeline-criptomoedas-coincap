# Importando Libs
from datetime import datetime, UTC
import os

import requests 
import pandas as pd
from pandas_gbq import to_gbq
from google.oauth2 import service_account
from dotenv import load_dotenv

# Carregando variáveis de ambiente
load_dotenv(dotenv_path='.env')

# Variáveis de ambiente
API_KEY = os.getenv("API_KEY")
GCP_CREDENTIALS = os.getenv("GCP_CREDENTIALS")
BQ_PROJECT_ID = os.getenv("BQ_PROJECT_ID")
BQ_BRZ_DATASET_ID = os.getenv("BQ_BRZ_DATASET_ID")
BQ_TABLE_ID = os.getenv("BQ_TABLE_ID ")

# Variáveis globais
current_utc_datetime = datetime.now(UTC)

# Função para enviar os dados coletados via API ao Google Big Query
def to_big_query(df, credentials, table_name):
    auth = service_account.Credentials.from_service_account_file(
        credentials, scopes=["https://www.googleapis.com/auth/bigquery"]
    )

    try:
        df.to_gbq( 
                destination_table = f'{BQ_PROJECT_ID}.{BQ_BRZ_DATASET_ID}.{table_name}',
                project_id = BQ_PROJECT_ID,
                credentials = auth,
                if_exists = 'append'
                )
        print("Dados inseridos no Big Query!")

    except Exception as e:
        print(f"Não foi possível inserir os dados no Big Query. Erro = {e}")

    return None


# Função para extração de dados históricos das criptomoedas especificadas(últimos 364 dias)
def get_asset_history():
    dfs = []
    
    assets = ["bitcoin", "ethereum", "solana", "chainlink", "aave", "xrp", "cardano", "dogecoin", "tron", "monero"]
    
    for asset_id in assets:             
        url_base = f"https://api.coincap.io/v2/assets/{asset_id}/history"  

        headers = {
            "Authorization": f"Bearer {API_KEY}"
        }

        params = {
            "interval": "d1"
        }

        try:
            fetch_data = requests.get(url=url_base, headers=headers, params=params)
            print(f"Conection status: {fetch_data.status_code}")
        except Exception as e:
            print(f"Error = {e}")

        raw_data = fetch_data.json()

        data = raw_data.get("data", {})

        df = pd.json_normalize(data)

        df["id"] = asset_id

        df["extracted_at"] = current_utc_datetime

        dfs.append(df)

    final_df = pd.concat(dfs)

    # Enviando dados ao Google Big Query
    to_big_query(df=final_df, credentials=GCP_CREDENTIALS, table_name='assets_history')

    print(final_df)

    return final_df


if __name__ == "__main__":#
    get_asset_history()











