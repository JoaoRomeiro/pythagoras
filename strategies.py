import pandas_ta as ta
import os


def moving_average_crossover(df):
    """
    Calcula os sinais para uma estratégia de cruzamento de médias móveis,
    mas agora com um filtro de tendência de longo prazo.
    """
    # Lê os períodos do ambiente
    fast_period = int(os.getenv("FAST_SMA_PERIOD", 20))
    slow_period = int(os.getenv("SLOW_SMA_PERIOD", 50))
    trend_filter_period = int(
        os.getenv("TREND_FILTER_PERIOD", 200))  # <-- Novo

    print(
        f"Calculating MA Crossover with Trend Filter (fast={fast_period}, slow={slow_period}, filter={trend_filter_period})...")

    # --- Calcula todas as três médias móveis ---
    df.ta.sma(length=fast_period, append=True)
    df.ta.sma(length=slow_period, append=True)
    df.ta.sma(length=trend_filter_period, append=True)  # <-- Novo

    # Renomeia as colunas dinamicamente para facilitar a leitura
    fast_sma_col = f'SMA_{fast_period}'
    slow_sma_col = f'SMA_{slow_period}'
    trend_filter_col = f'SMA_{trend_filter_period}'  # <-- Novo

    df.dropna(inplace=True)
    df['signal'] = 0

    # --- Lógica de Sinal com o Filtro ---

    # Condição de cruzamento de compra
    buy_crossover = (df[fast_sma_col].shift(1) > df[slow_sma_col].shift(1)) & \
                    (df[fast_sma_col].shift(2) < df[slow_sma_col].shift(2))

    # Condição de cruzamento de venda
    sell_crossover = (df[fast_sma_col].shift(1) < df[slow_sma_col].shift(1)) & \
                     (df[fast_sma_col].shift(2) > df[slow_sma_col].shift(2))

    # Condição de filtro de tendência
    uptrend_filter = df['close'] > df[trend_filter_col]
    downtrend_filter = df['close'] < df[trend_filter_col]

    # Um sinal de compra SÓ é válido se o cruzamento de compra ocorrer E a tendência for de alta
    df.loc[buy_crossover & uptrend_filter, 'signal'] = 1

    # Um sinal de venda SÓ é válido se o cruzamento de venda ocorrer E a tendência for de baixa
    df.loc[sell_crossover & downtrend_filter, 'signal'] = -1

    return df
