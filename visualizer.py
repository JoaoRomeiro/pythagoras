import mplfinance as mpf

def plot_ohlc(df, asset_name='Asset'):
    """
    Plota um gráfico de velas (candlestick) a partir de um DataFrame OHLC.
    
    Args:
        df (pd.DataFrame): DataFrame com colunas 'open', 'high', 'low', 'close', 'volume'.
        asset_name (str): O nome do ativo para usar no título do gráfico.
    """
    print("Generating plot...")
    
    # Plota os últimos 100 candles para uma visualização mais limpa
    data_to_plot = df.tail(100)
    
    mpf.plot(data_to_plot, 
             type='candle', 
             style='yahoo',
             title=f'{asset_name} - Last 100 Candles',
             ylabel='Price',
             volume=True,
             panel_ratios=(3, 1))
             
    print("Plot window opened. Close the window to continue.")