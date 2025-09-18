import os
import pandas as pd
from dotenv import load_dotenv

def analyze_data():
    """
    Carrega o arquivo Parquet gerado pelo script de download,
    usando as configurações do arquivo .env para encontrar o arquivo correto,
    e exibe uma análise inicial dos dados.
    """
    # Carrega as variáveis do arquivo .env para o ambiente atual
    load_dotenv()

    # Pega as configurações do ambiente para montar o nome do arquivo
    asset = os.getenv("ASSET")
    start_date = os.getenv("START_DATE")
    end_date = os.getenv("END_DATE")

    # Verifica se as variáveis foram carregadas corretamente
    if not all([asset, start_date, end_date]):
        print("Error: ASSET, START_DATE, or END_DATE variables not found in the .env file.")
        return

    # 1. Monta o nome do arquivo dinamicamente
    filename = f"{asset}-{start_date}-{end_date}.parquet"
    
    # 2. Cria o caminho completo para o arquivo de forma segura
    file_path = os.path.join('data', filename)

    print(f"Attempting to load file: {file_path}")

    # 3. Verifica se o arquivo realmente existe antes de tentar carregá-lo
    if not os.path.exists(file_path):
        print("\nERROR: File not found!")
        print("Please run the download script first:")
        print("python main.py download")
        return

    try:
        # 4. Carrega o arquivo Parquet para um DataFrame do pandas
        df_ticks = pd.read_parquet(file_path)

        print("\nFile loaded successfully!")
        
        # 5. Exibe informações e amostras dos dados
        print("\n--- General DataFrame Information ---")
        df_ticks.info()

        print("\n--- Data Sample (First 5 Rows) ---")
        print(df_ticks.head())

        print("\n--- Data Sample (Last 5 Rows) ---")
        print(df_ticks.tail())

    except Exception as e:
        print(f"\nAn error occurred while reading the Parquet file: {e}")