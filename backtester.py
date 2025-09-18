import pandas as pd
import os
from tqdm import tqdm  # 1. Importa a biblioteca

# A função run_trade_simulation permanece a mesma
def run_trade_simulation(ticks_df, entry_time, exit_time, entry_price, stop_loss, take_profit, trade_type='buy'):
    # ... (código inalterado)
    trade_ticks = ticks_df[(ticks_df['time'] > entry_time) & (ticks_df['time'] <= exit_time)]

    if trade_ticks.empty:
        return {'status': 'NO_TICKS', 'exit_price': None, 'exit_time': None}

    for index, tick in trade_ticks.iterrows():
        price_to_check = tick['bid'] if trade_type == 'buy' else tick['ask']

        if trade_type == 'buy':
            if price_to_check <= stop_loss:
                return {'status': 'STOP_LOSS', 'exit_price': stop_loss, 'exit_time': tick['time']}
            if price_to_check >= take_profit:
                return {'status': 'TAKE_PROFIT', 'exit_price': take_profit, 'exit_time': tick['time']}
        elif trade_type == 'sell':
            if price_to_check >= stop_loss:
                return {'status': 'STOP_LOSS', 'exit_price': stop_loss, 'exit_time': tick['time']}
            if price_to_check <= take_profit:
                return {'status': 'TAKE_PROFIT', 'exit_price': take_profit, 'exit_time': tick['time']}
            
    last_tick_price = trade_ticks.iloc[-1]['bid'] if trade_type == 'buy' else trade_ticks.iloc[-1]['ask']
    return {'status': 'TIME_LIMIT', 'exit_price': last_tick_price, 'exit_time': trade_ticks.iloc[-1]['time']}


def run_backtest(ohlc_df, ticks_df, strategy_func):
    print("Starting backtest...")
    
    stop_loss_pips = int(os.getenv("STOP_LOSS_PIPS", 200))
    take_profit_pips = int(os.getenv("TAKE_PROFIT_PIPS", 400))
    trade_volume_lots = float(os.getenv("TRADE_VOLUME_LOTS", 0.1))

    pip_value_per_lot = 10 
    pip_value_monetary = pip_value_per_lot * trade_volume_lots
    
    ohlc_with_signals = strategy_func(ohlc_df.copy())
    
    trades = []
    pip_value_points = 0.0001
    position = None

    # 2. "Envelopa" o range do loop com tqdm para criar a barra de progresso
    for i in tqdm(range(1, len(ohlc_with_signals)), desc="Backtesting Progress"):
        current_candle = ohlc_with_signals.iloc[i]
        signal = current_candle['signal']
        
        if position is not None:
            trade_result = run_trade_simulation(
                ticks_df, ohlc_with_signals.iloc[i-1].name, current_candle.name,
                position['entry_price'], position['stop_loss'], position['take_profit'], position['type']
            )
            if trade_result['status'] in ['STOP_LOSS', 'TAKE_PROFIT']:
                position.update(trade_result)
                trades.append(position)
                position = None
            elif (position['type'] == 'buy' and signal == -1) or \
                 (position['type'] == 'sell' and signal == 1):
                position['exit_price'] = current_candle['open']
                position['exit_time'] = current_candle.name
                position['status'] = 'SIGNAL_EXIT'
                trades.append(position)
                position = None

        if position is None:
            if signal == 1 or signal == -1:
                entry_price = current_candle['open']
                trade_type = 'buy' if signal == 1 else 'sell'
                
                if trade_type == 'buy':
                    stop_loss = entry_price - (stop_loss_pips * pip_value_points)
                    take_profit = entry_price + (take_profit_pips * pip_value_points)
                else: # Venda
                    stop_loss = entry_price + (stop_loss_pips * pip_value_points)
                    take_profit = entry_price - (take_profit_pips * pip_value_points)
                
                position = {
                    'entry_price': entry_price, 'entry_time': current_candle.name,
                    'stop_loss': stop_loss, 'take_profit': take_profit, 'type': trade_type
                }
    
    if not trades:
        print("Backtest finished. No trades were executed.")
        return None

    results_df = pd.DataFrame(trades)
    
    buy_pnl = (results_df['exit_price'] - results_df['entry_price']) / pip_value_points
    sell_pnl = (results_df['entry_price'] - results_df['exit_price']) / pip_value_points
    results_df['pnl_points'] = buy_pnl.where(results_df['type'] == 'buy', sell_pnl)
    
    results_df['pnl_usd'] = results_df['pnl_points'] * pip_value_monetary

    print("\nBacktest finished.") # Adicionei uma quebra de linha para separar da barra de progresso
    return results_df