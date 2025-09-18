# download.py

import MetaTrader5 as mt5
import pandas as pd
import os
from datetime import datetime
import time

def download_ticks():
    """
    Baixa os ticks de um ativo em um determinado período em lotes (mês a mês) 
    e os salva em um único arquivo Parquet.
    """
    asset = os.getenv("ASSET")
    start_date_str = os.getenv("START_DATE")
    end_date_str = os.getenv("END_DATE")
    
    print(f"\nIniciando download de ticks para o ativo {asset}...")
    print(f"Período total: de {start_date_str} a {end_date_str}")
    
    try:
        # Gera um range de datas, um mês de cada vez
        date_range = pd.date_range(start=start_date_str, end=end_date_str, freq='MS') # MS = Month Start
        all_ticks_df = []

        for i in range(len(date_range)):
            start_period = date_range[i]
            # O fim do período é o início do próximo mês, ou a data final do request
            if i + 1 < len(date_range):
                end_period = date_range[i+1]
            else:
                end_period = datetime.strptime(end_date_str, "%Y-%m-%d")

            print(f"  -> Baixando lote: {start_period.strftime('%Y-%m-%d')} a {end_period.strftime('%Y-%m-%d')}")
            
            # Faz a requisição dos ticks ao MT5 para o período menor
            ticks = mt5.copy_ticks_range(asset, start_period, end_period, mt5.COPY_TICKS_ALL)
            
            if ticks is None or len(ticks) == 0:
                print(f"     Nenhum tick encontrado para este lote. Erro MT5: {mt5.last_error()}")
            else:
                print(f"     {len(ticks)} ticks recebidos.")
                all_ticks_df.append(pd.DataFrame(ticks))
            
            # Pequena pausa para não sobrecarregar o terminal
            time.sleep(1) 

        if not all_ticks_df:
            print("Nenhum dado foi baixado no período total especificado.")
            return

        print("\nJuntando todos os lotes de dados...")
        final_df = pd.concat(all_ticks_df, ignore_index=True)
        
        print("Convertendo coluna de tempo e removendo duplicatas...")
        final_df['time'] = pd.to_datetime(final_df['time'], unit='s')
        final_df.drop_duplicates(inplace=True)
        
        if not os.path.exists('data'):
            os.makedirs('data')
            
        output_filename = f"{asset}-{start_date_str}-{end_date_str}.parquet"
        output_path = os.path.join('data', output_filename)
        
        print(f"Salvando {len(final_df)} ticks em {output_path}...")
        final_df.to_parquet(output_path, index=False)
        
        print("\nDownload e armazenamento concluídos com sucesso!")
        
    except Exception as e:
        print(f"Ocorreu um erro durante o processo de download: {e}")