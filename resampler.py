import pandas as pd

def resample_to_ohlc(df_ticks, timeframe='1H'):
    """
    Converte um DataFrame de ticks em um DataFrame de velas OHLC.
    
    Args:
        df_ticks (pd.DataFrame): O DataFrame contendo os dados de ticks.
        timeframe (str): O intervalo de tempo para as velas (ex: '1T' para 1 min, '1H' para 1 hora).
        
    Returns:
        pd.DataFrame: Um novo DataFrame com os dados em formato OHLC.
    """
    # 1. Define a coluna 'time' como o índice, que é um requisito para a reamostragem (resample)
    df_indexed = df_ticks.set_index('time')

    # 2. Define a lógica de agregação de forma explícita
    # 'first': primeiro preço -> Open
    # 'max': preço máximo -> High
    # 'min': preço mínimo -> Low
    # 'last': último preço -> Close
    # 'count': contagem de ticks -> Volume
    logic = {
        'bid': ['first', 'max', 'min', 'last', 'count']
    }

    # 3. Aplica a reamostragem e a agregação em uma única chamada (esta é a correção principal)
    df_ohlc = df_indexed.resample(timeframe).agg(logic)

    # 4. Remove o nível superior das colunas gerado pelo .agg()
    df_ohlc.columns = df_ohlc.columns.droplevel(0)

    # 5. Renomeia as colunas para o padrão OHLC
    df_ohlc.rename(columns={
        'first': 'open',
        'max': 'high',
        'min': 'low',
        'last': 'close',
        'count': 'volume'
    }, inplace=True)
    
    # 6. Remove as linhas que não tiveram ticks (ex: fins de semana, feriados)
    df_ohlc.dropna(inplace=True)

    return df_ohlc