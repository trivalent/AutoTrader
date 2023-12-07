from autotrader import AutoData
from autotrader.brokers.broker_utils import BrokerUtils
from autotrader.brokers.broker import AbstractBroker
from autotrader.brokers.finvasia.utils import Utils
from autotrader.brokers.trading import Order, IsolatedPosition, Position


class Broker(AbstractBroker):
    def __init__(self, finvasiaConfig: dict, utils: BrokerUtils = None) -> None:
        self.utils = Utils()
        if not self.utils.doLogin():
            raise Exception("Unable to Login, please check your credentials")
        self.autodata = AutoData(data_source="finvasia", finvasia_api=self.utils,
                                 live_price=None, tokens=None)

    def __repr__(self):
        return "AutoTrader-Finvasia Broker Interface"

    def __str__(self):
        return "AutoTrader-Finvasia Broker Interface"

    def get_NAV(self, *args, **kwargs) -> float:
        return self.utils.getNav()

    def get_balance(self, *args, **kwargs) -> float:
        return self.utils.getBalance()

    def place_order(self, order: Order, *args, **kwargs) -> None:
        return self.utils.placeOrder(order, *args, **kwargs)

    def get_orders(self, instrument: str = None, *args, **kwargs) -> dict:
        return self.utils.getAllOrders(instrument, *args, **kwargs)

    def cancel_order(self, order_id: int, *args, **kwargs) -> None:
        return self.utils.cancelOrder(order_id, *args, **kwargs)

    def get_trades(self, instrument: str = None, *args, **kwargs) -> dict:
        return self.utils.getTrades(instrument, *args, **kwargs)

    def get_positions(self, instrument: str = None, *args, **kwargs) -> dict:
        return self.utils.getPositions(instrument, *args, **kwargs)
