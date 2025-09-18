# mt5_connector.py

import MetaTrader5 as mt5
import os
from dotenv import load_dotenv

def connect():
    """
    Carrega as variáveis de ambiente e inicializa a conexão com o MetaTrader 5.
    """
    # Carrega as variáveis do arquivo .env
    load_dotenv()
    
    login = int(os.getenv("MT5_LOGIN"))
    password = os.getenv("MT5_PASSWORD")
    server = os.getenv("MT5_SERVER")
    
    print("Iniciando conexão com o MetaTrader 5...")
    
    # Inicializa a conexão
    if not mt5.initialize():
        print(f"Falha na inicialização do MT5, erro: {mt5.last_error()}")
        return False
        
    # Tenta realizar o login
    if not mt5.login(login, password, server):
        print(f"Falha no login, erro: {mt5.last_error()}")
        mt5.shutdown()
        return False
    
    print(f"Conexão bem-sucedida ao servidor {server}.")
    return True

def disconnect():
    """
    Encerra a conexão com o MetaTrader 5.
    """
    print("Encerrando a conexão com o MetaTrader 5.")
    mt5.shutdown()