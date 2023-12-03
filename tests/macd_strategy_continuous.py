import os
from finta import TA
import autotrader.indicators as indicators
from autotrader.brokers.trading import Order


class SimpleMACD:
    """Simple MACD Strategy

    Rules
    ------
    1. Trade in direction of trend, as per 200EMA.
    2. Entry signal on MACD cross below/above zero line.
    3. Set stop loss at recent price swing.
    4. Target 1.5 take profit.
    """

    def __init__(self, parameters, data, instrument):
        """Define all indicators used in the strategy."""

        self.name = "MACD Trend Strategy"
        self.params = parameters
        self.instrument = instrument

        # Initial feature calculation
        self.calculate_features(data)

        # Construct indicators dict for plotting
        self.indicators = {
            "MACD (12/26/9)": {
                "type": "MACD",
                "macd": self.MACD.MACD,
                "signal": self.MACD.SIGNAL,
            },
            "EMA (200)": {"type": "MA", "data": self.ema},
        }

    def calculate_features(self, data):
        # Save data for other functions
        self.data = data

        # 200EMA
        self.ema = TA.EMA(data, self.params["ema_period"])

        # MACD
        self.MACD = TA.MACD(
            data,
            self.params["MACD_fast"],
            self.params["MACD_slow"],
            self.params["MACD_smoothing"],
        )
        self.MACD_CO = indicators.crossover(self.MACD.MACD, self.MACD.SIGNAL)
        self.MACD_CO_vals = indicators.cross_values(
            self.MACD.MACD, self.MACD.SIGNAL, self.MACD_CO
        )

        # Price swings
        self.swings = indicators.find_swings(data)

        # TODO - append data to self.indicators as it is created

    def generate_signal(self, data):
        """Define strategy to determine entry signals."""
        # Feature calculation
        self.calculate_features(data)
        i = -1  # Now just use latest data
        if (
            self.data.Close.values[i] > self.ema[i]
            and self.MACD_CO[i] == 1
            and self.MACD_CO_vals[i] < 0
        ):
            exit_dict = self.generate_exit_levels(signal=1, i=i)
            new_order = Order(
                direction=1,
                stop_loss=exit_dict["stop_loss"],
                take_profit=exit_dict["take_profit"],
            )

        elif (
            self.data.Close.values[i] < self.ema[i]
            and self.MACD_CO[i] == -1
            and self.MACD_CO_vals[i] > 0
        ):
            exit_dict = self.generate_exit_levels(signal=-1, i=i)
            new_order = Order(
                direction=-1,
                stop_loss=exit_dict["stop_loss"],
                take_profit=exit_dict["take_profit"],
            )

        else:
            new_order = Order()

        return new_order

    def generate_exit_levels(self, signal, i):
        """Function to determine stop loss and take profit levels."""
        stop_type = "limit"
        RR = self.params["RR"]

        if signal == 0:
            stop = None
            take = None
        else:
            if signal == 1:
                stop = self.swings.Lows[i]
                take = self.data.Close[i] + RR * (self.data.Close[i] - stop)
            else:
                stop = self.swings.Highs[i]
                take = self.data.Close[i] - RR * (stop - self.data.Close[i])

        exit_dict = {"stop_loss": stop, "stop_type": stop_type, "take_profit": take}

        return exit_dict


# if __name__ == "__main__":
from autotrader.autotrader import AutoTrader

config = {
    "NAME": "MACD Strategy",
    "MODULE": "macd_strategy_continuous",
    "CLASS": "SimpleMACD",
    "INTERVAL": "4h",
    "PERIOD": 300,
    "RISK_PC": 1.5,
    "SIZING": "risk",
    "PARAMETERS": {
        "ema_period": 200,
        "MACD_fast": 5,
        "MACD_slow": 19,
        "MACD_smoothing": 9,
        "RR": 1.5,
    },
    "WATCHLIST": ["EUR_USD"],
}
home_dir = os.getcwd()
data_dir = "/home/kmackle/Documents/AutoTrader/tests/data"

at = AutoTrader()
at.configure(verbosity=1, show_plot=True)  # , broker='ccxt:binanceusdm, ccxt:bitmex')
at.add_strategy(config_dict=config, strategy=SimpleMACD)
at.plot_settings(show_cancelled=False)
at.add_data(
    {"EUR_USD": "EUR_USD_H4.csv"},
    # data_directory=os.path.join(home_dir, 'data'),
    data_directory=data_dir,
)
at.backtest(
    start="1/1/2021",
    end="1/6/2021",
)
at.virtual_account_config(
    # initial_balance=1000, leverage=30, spread=0.5 * 1e-4, commission=0.005
    initial_balance=1000,
    leverage=30,
    spread=0,
    commission=0.0,
    # exchange='ccxt:binanceusdm',
    # tradeable_instruments=["EUR_USD"],
)
at.virtual_account_config(
    # initial_balance=1000, leverage=30, spread=0.5 * 1e-4, commission=0.005
    initial_balance=1000,
    leverage=30,
    spread=0,
    commission=0.0,
    # exchange='ccxt:bitmex',
    # tradeable_instruments=["EUR_USD"],
)
at.run()

# bot = at.get_bots_deployed()
