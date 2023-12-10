import pyotp as pyotp

from autotrader import Order, Trade
from autotrader.brokers.broker_utils import BrokerUtils
from NorenRestApiPy.NorenApi import NorenApi


class Utils(BrokerUtils, NorenApi):
    def __init__(self, **kwargs):
        NorenApi.__init__(self, host="https://api.shoonya.com/NorenWClientTP/",
                          websocket="wss://api.shoonya.com/NorenWSTP/")
        self.finvasiaAPI = self
        # TOTP_KEY: "L5D7ABHZH2XG7BB4MQ5EG632GRK3P66H"
        # TOTP_INTERVAL: 30
        # # your API Key obtained from Shoonya/Finvasia
        # API_KEY: ""
        # USER_NAME: ""
        # PASSWORD: ""
        if "config" not in kwargs:
            finvasiaConfig = kwargs['global_config']['finvasia']
        else:
            finvasiaConfig = kwargs['config']

        self.totpKey = finvasiaConfig['TOTP_KEY']
        self.totpInterval = finvasiaConfig['TOTP_INTERVAL']
        self.apiKey = finvasiaConfig['API_KEY']
        self.userName = finvasiaConfig['USER_NAME']
        self.password = finvasiaConfig['PASSWORD']
        self.vendorCode = self.userName + '_U'
        self.imei = "XG7BB4MQ5EG6"

    def doLogin(self) -> bool:
        login = self.finvasiaAPI.login(userid=self.userName, password=self.password,
                                       twoFA=pyotp.TOTP(self.totpKey, interval=self.totpInterval).now(),
                                       vendor_code=self.vendorCode, api_secret=self.apiKey, imei=self.imei)
        return login['stat'] == "Ok"

    def getNav(self) -> float:
        """Fetch all the holding and calculate the Nav"""
        response = self.finvasiaAPI.get_holdings()
        nav = 0.0
        status = response[0]['stat'] == "Ok"
        if status:
            print("Successfully fetched holding")
            for entry in response:
                price = float(entry['upldprc'])
                if price == 0:
                    print(f"Price for Symbol: {entry['exch_tsym'][0]['tsym']} is 0. setting it to 1")
                    price = 1;
                nav += price * float(entry['holdqty'])
        else:
            print("fetch holding failed")

        print("Total Asset value of the portfolio is = ", nav)
        return nav

    def getBalance(self):
        response = self.finvasiaAPI.get_limits()
        status = response['stat'] == "Ok"
        balance = 0.0
        if status:
            print("Successfully fetched limits")
            balance = response['cash']
        else:
            print("Failed to fetch balance")
        return balance

    def placeOrder(self, order: Order, *args, **kwargs):
        finvasiaOrder = self.finvasiaOrderFromOrder(order)
        response = self.finvasiaAPI.place_order(
            buy_or_sell=finvasiaOrder['buy_or_sell'],
            price_type=finvasiaOrder['price_type'],
            quantity=finvasiaOrder['quantity'],
            price=finvasiaOrder['price'],
            exchange=order.exchange,
            trigger_price=finvasiaOrder['trigger_price'])
        return self.orderFromFinvasiaOrder(order, response)

    def getAllOrders(self, instrument, *args, **kwargs) -> dict:
        response = self.finvasiaAPI.get_order_book()
        orders = {}
        for order in response:
            nativeOrder = self.convertToNativeOrder(order)
            orders[nativeOrder.id] = nativeOrder
        finalOrders = {k: v for k, v in orders.items() if v.status == 'pending'}
        return finalOrders

    def cancelOrder(self, order_id, *param, **param1):
        return self.finvasiaAPI.cancel_order(order_id)

    def getTrades(self, instrument=None, *param, **param1) -> dict:
        response = self.finvasiaAPI.get_trade_book()
        trades = {}
        for trade in response:
            nativeTrade = self.finvasiaTradesToTrades(trade)
            trades[nativeTrade.id] = nativeTrade
        return trades

    def getPositions(self, instrument=None, *param, **param1):
        response = self.finvasiaAPI.get_positions()
        return response

    @staticmethod
    def finvasiaTradesToTrades(finvasiaOrder) -> Trade:
        direction = 1 if finvasiaOrder['trantype'] == "B" else -1
        return Trade(
            id=finvasiaOrder['norenorderno'],
            direction=direction,
            instrument=finvasiaOrder['tsym'],
            order_price=None,
            order_time=None,
            order_type=None,
            size=float(finvasiaOrder['qty']),
            fill_time=finvasiaOrder["fltm"],
            fill_price=float(finvasiaOrder["flprc"]),
            fill_direction=direction,
        )

    @staticmethod
    def convertToNativeOrder(finvasiaOrder) -> Order:
        direction = 1 if finvasiaOrder['trantype'] == "B" else -1
        price_type = finvasiaOrder['prctyp']
        order_type = 'market'
        status = "pending"
        if finvasiaOrder['status'] == "REJECT":
            status = "cancelled"

        limitprice = None
        orderPrice = None

        if price_type == 'LMT':
            order_type = 'limit'
            limitprice = finvasiaOrder['prc']
        elif price_type == 'SL-LMT':
            order_type = 'stop-limit'
            limitprice = finvasiaOrder['prc']
        elif price_type == 'SL-MKT':
            order_type = 'stop'
            orderPrice = finvasiaOrder['prc']
        else:
            orderPrice = finvasiaOrder['prc']

        return Order(id=finvasiaOrder['norenorderno'],
                     direction=direction,
                     instrument=finvasiaOrder['tsym'],
                     order_type=order_type,
                     size=float(finvasiaOrder['qty']),
                     order_stop_price=orderPrice,
                     order_limit_price=limitprice,
                     status=status)

    @staticmethod
    def orderFromFinvasiaOrder(originalOrder: Order, finvasiaResponse) -> dict:
        originalOrder.id = finvasiaResponse['norenordno']
        originalOrder.status = "open" if finvasiaResponse['stat'] == 'Ok' else "cancelled"
        return originalOrder.as_dict()

    @staticmethod
    def finvasiaOrderFromOrder(order: Order) -> dict:
        finOrder = {'buy_or_sell': 'B' if order.direction == 1 else 'S',
                    'quantity': order.size,
                    'trigger_price': None,
                    'price': 0.0}

        if order.order_type == 'limit':
            price_type = 'LMT'
        elif order.order_type == 'stop-limit':
            price_type = 'SL-LMT'
        elif order.order_type == 'stop':
            price_type = 'SL-MKT'
        else:
            price_type = 'MKT'
        finOrder['price_type'] = price_type

        if order.order_type == "stop" or order.order_type == "stop-limit":
            finOrder['trigger_price'] = order.order_limit_price

        if order.order_type == "limit" or order.order_type == 'stop-limit':
            finOrder['price'] = order.order_price

        return finOrder
