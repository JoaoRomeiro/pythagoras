import sys
import os
from dotenv import load_dotenv
import pandas as pd

import mt5_connector
import download
import analyzer
import resampler
import visualizer
import backtester   # Novo import
import strategies   # Novo import

def main():
    """
    Ponto de entrada principal do script.
    Verifica os argumentos da linha de comando e executa a tarefa correspondente.
    """
    load_dotenv() # Garante que as variáveis de ambiente estão disponíveis

    if len(sys.argv) < 2:
        print("Command not specified. Usage: python main.py <command>")
        print("Available commands: download, analyze, resample, plot, backtest") # Atualizado
        return
        
    command = sys.argv[1].lower()
    
    if command == "download":
        connection_successful = False
        try:
            connection_successful = mt5_connector.connect()
            if connection_successful:
                download.download_ticks()
        finally:
            if connection_successful:
                mt5_connector.disconnect()
                
    elif command == "analyze":
        analyzer.analyze_data()
        
    elif command == "resample":
        # Mapeamento de timeframes do padrão MT5 para o padrão Pandas
        timeframe_mapping = {
            'M1': '1min', 'M5': '5min', 'M15': '15min', 'M30': '30min',
            'H1': '1h', 'H4': '4h', 'D1': '1d', 'W1': '1w', 'MN': 'MS'
        }
        # Carrega as configurações do .env
        asset = os.getenv("ASSET")
        start_date = os.getenv("START_DATE")
        end_date = os.getenv("END_DATE")
        timeframe_mt5 = os.getenv("TIMEFRAME")

        # Valida o timeframe informado no .env
        if timeframe_mt5 not in timeframe_mapping:
            print(f"ERROR: Timeframe '{timeframe_mt5}' in .env file is not valid.")
            print(f"Please use one of the following: {', '.join(timeframe_mapping.keys())}")
            return
            
        timeframe_pd = timeframe_mapping[timeframe_mt5]
        ticks_filename = f"{asset}-{start_date}-{end_date}.parquet"
        ticks_filepath = os.path.join('data', ticks_filename)

        if not os.path.exists(ticks_filepath):
            print(f"ERROR: Ticks file not found at {ticks_filepath}")
            print("Please run 'python main.py download' first.")
            return

        print(f"Loading ticks from {ticks_filepath}...")
        df_ticks = pd.read_parquet(ticks_filepath)
        
        print(f"Resampling ticks to {timeframe_mt5} timeframe (using pandas '{timeframe_pd}')...")
        df_ohlc = resampler.resample_to_ohlc(df_ticks, timeframe_pd)

        ohlc_filename = f"{asset}-{start_date}-{end_date}-{timeframe_mt5}.parquet"
        ohlc_filepath = os.path.join('data', ohlc_filename)
        
        print(f"Saving OHLC data to {ohlc_filepath}...")
        df_ohlc.to_parquet(ohlc_filepath)

        print("\nResampling complete and file saved successfully!")
        print("\n--- OHLC Data Sample (First 5 Rows) ---")
        print(df_ohlc.head())

    elif command == "plot":
        asset = os.getenv("ASSET")
        start_date = os.getenv("START_DATE")
        end_date = os.getenv("END_DATE")
        timeframe_mt5 = os.getenv("TIMEFRAME")

        ohlc_filename = f"{asset}-{start_date}-{end_date}-{timeframe_mt5}.parquet"
        ohlc_filepath = os.path.join('data', ohlc_filename)
        
        if not os.path.exists(ohlc_filepath):
            print(f"ERROR: OHLC file not found at {ohlc_filepath}")
            print("Please run 'python main.py resample' first.")
            return

        print(f"Loading OHLC data from {ohlc_filepath}...")
        df_ohlc = pd.read_parquet(ohlc_filepath)

        visualizer.plot_ohlc(df_ohlc, asset) 

    # --- NOVO COMANDO BACKTEST ---
    elif command == "backtest":
        asset = os.getenv("ASSET")
        start_date = os.getenv("START_DATE")
        end_date = os.getenv("END_DATE")
        timeframe_mt5 = os.getenv("TIMEFRAME")

        # Define os caminhos dos arquivos de entrada
        ohlc_filename = f"{asset}-{start_date}-{end_date}-{timeframe_mt5}.parquet"
        ohlc_filepath = os.path.join('data', ohlc_filename)
        
        ticks_filename = f"{asset}-{start_date}-{end_date}.parquet"
        ticks_filepath = os.path.join('data', ticks_filename)
        
        if not os.path.exists(ohlc_filepath) or not os.path.exists(ticks_filepath):
            print("ERROR: Input files not found. Please run 'download' and 'resample' first.")
            return

        print("Loading OHLC and Ticks data...")
        df_ohlc = pd.read_parquet(ohlc_filepath)
        df_ticks = pd.read_parquet(ticks_filepath)

        # "Pluga" a estratégia desejada no motor de backtest.
        results = backtester.run_backtest(df_ohlc, df_ticks, strategies.moving_average_crossover)
        
        if results is None:
            return

        # Exibe um resumo simples dos resultados
        print("\n--- Backtest Results Summary ---")
        print(f"Total Trades: {len(results)}")
        print(f"Winning Trades: {len(results[results['pnl_points'] > 0])}")
        print(f"Losing Trades: {len(results[results['pnl_points'] <= 0])}")
        
        total_pnl_pips = results['pnl_points'].sum()
        total_pnl_usd = results['pnl_usd'].sum() # Novo
        print(f"Total PnL (in pips): {total_pnl_pips:.2f}")
        print(f"Total PnL (in USD for {os.getenv('TRADE_VOLUME_LOTS')} lots): ${total_pnl_usd:.2f}") # Novo

        print("\n--- Last 5 Trades ---")
        print(results.tail())
        
    else:
        print(f"Command '{command}' unknown.")
        print("Available commands: download, analyze, resample, plot, backtest") # Atualizado

if __name__ == "__main__":
    main()