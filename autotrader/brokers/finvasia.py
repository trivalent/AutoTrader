import time
import time
from datetime import datetime, timedelta

import pandas as pd
from numpy.f2py.auxfuncs import throw_error
from pandas.core.ops import get_op_result_name

from autotrader.brokers.broker import Broker
from autotrader.brokers.trading import Order, Position, Trade
from autotrader.utilities import get_logger

try:
    from NorenRestApiPy.NorenApi import NorenApi
except ImportError:
    raise Exception("Please install finvasia python package from https://github.com/Shoonya-Dev/ShoonyaApi-py")

class API(NorenApi):
    def __init__(self):
        NorenApi.__init__(self, host="https://api.shoonya.com/NorenWClientTP/",
                          websocket="wss://api.shoonya.com/NorenWSTP/")

class Broker(Broker):

    def __init__(self, config: dict):
        """Create Finvasia API context."""
        # TOTP_KEY: "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
        # TOTP_INTERVAL: 30
        # # your API Key obtained from Shoonya/Finvasia
        # API_KEY: ""
        # USER_NAME: ""
        # PASSWORD: ""

        print(config)
        self.isLoggedIn = False
        self.totpKey = config['global_config']['finvasia']['TOTP_KEY']
        self.totpInterval = config['global_config']['finvasia']['TOTP_INTERVAL']
        self.apiKey = config['global_config']['finvasia']['API_KEY']
        self.userName = config['global_config']['finvasia']['USER_NAME']
        self.password = config['global_config']['finvasia']['PASSWORD']
        self.vendorCode = self.userName + '_U'
        self.imei = "XG7BB4MQ5EG6"

        self.api = API()
        self.open_positions = {}

        # Assign data broker
        self._data_broker = self
        self.nseData = pd.DataFrame()

        # initialize logger
        self._logging_options = config["logging_options"]
        self._logger = get_logger(name="finvasia_broker", **self._logging_options)

    def __repr__(self):
        return "AutoTrader-Finvasia Broker Interface"

    def __str__(self):
        return "AutoTrader-Finvasia Broker Interface"

    @property
    def data_broker(self):
        return self._data_broker

    def get_NAV(self) -> float:
        """Returns Net Asset Value of account."""
        self._check_connection()
        response = self.api.get_holdings()
        nav = 0.0
        status = response[0]['stat'] == "Ok"
        if status:
            for entry in response:
                price = float(entry['upldprc'])
                if price == 0:
                    print(f"Price for Symbol: {entry['exch_tsym'][0]['tsym']} is 0. setting it to 1")
                    price = 1;
                nav += price * float(entry['holdqty'])
        else:
            self._logger.error("Failed to fetch holdings")

        self._logger.info(f"Total Nav -> {nav}")
        return nav

    def get_balance(self) -> float:
        """Returns account balance."""
        self._check_connection()
        response = self.api.get_limits()
        status = response['stat'] == "Ok"
        balance = 0.0
        if status:
            print("Successfully fetched limits")
            balance = response['cash']
        else:
            self._logger.error("Failed to fetch balance")

        self._logger.info(f"Total Cash Balance -> {balance}")
        return balance

    def place_order(self, order: Order, **kwargs):
        """Submits order to broker."""
        self._check_connection()

        # Call order to set order time
        order()

        self._logger.error(f'Request received to place order -> {order}')
        # todo: Implement order processing
        return NotImplementedError("Order processing is not enabled yet")

        # Submit order
        # if order.order_type == "close":
        #     response = self._close_position(order.instrument)
        # elif order.order_type == "modify":
        #     response = self._modify_trade(order)
        # else:
        #     # for market/limit/stop-limit
        #     fin_order = self.order_to_finvasia_order(order)
        #     response = self.api.place_order(self,
        #                                     buy_or_sell=fin_order['buy_or_sell'],
        #                                     price_type=fin_order['price_type'],
        #                                     quantity=fin_order['quantity'],
        #                                     price=fin_order['price'],
        #                                     exchange=order.exchange,
        #                                     trigger_price=fin_order['trigger_price'])
        #     response = self.finvasia_res_to_order(order, response)
        #     print("Order type not recognised.")
        #
        # return response

    def get_orders(self, instrument=None, **kwargs) -> dict:
        """Get all pending orders in the account."""
        # TODO: Skip take profit and Stop loss order
        return self._get_my_orders('PENDING')

    def cancel_order(self, order_id: int, **kwargs) -> None:
        """Cancels pending order by ID."""
        self._check_connection()
        self.api.cancel_order( order_id)

    def get_trades(self, instrument=None, **kwargs) -> list:
        """Returns the trades (fills) made by the account."""
        self._check_connection()
        response = self.api.get_trade_book()
        trades = {}
        for trade in response:
            native_trade = self.finvasia_trades_to_trades(trade)
            trades[native_trade.id] = native_trade

        return list(trades)

    def get_positions(self, instrument: str = None, **kwargs) -> dict:
        """Gets the current positions open on the account."""
        self._check_connection()
        response = self.api.get_positions()

        _is_success = True
        # return an array object. Check if it is a failure.
        try:
            _is_success = response['stat'] != 'Not_Ok'
        except:
            _is_success = True

        if not _is_success:
            self._logger.error("Error retrieving positions")
            return {}

        oanda_open_positions = response
        open_positions = {}
        for position in oanda_open_positions:
            # since there are no POSITION IDs here, are creating it manually basis tsym, qty and L/S

            pos = {"instrument": position['tsym'],
                   "long_units": position['netqty'] if int(position['netqty']) > 0 else 0,
                   "long_PL": position['rpnl'] if int(position['netqty']) > 0 else 0, "long_margin": None,
                   "short_units": position['netqty'] if int(position['netqty']) < 0 else 0,
                   "short_PL": position['rpnl'] if int(position['netqty']) < 0 else 0, "short_margin": None,
                   "total_margin": position['netqty'] * position['netavgprc'],
                   "trade_IDs": position['tsym'] + "_" + position['netqty'] + "_" + "L" if int(
                       position['netqty']) > 0 else "S"}

            # fetch trade ID'strade_IDs
            # trade_IDs = []
            # if abs(pos["long_units"]) > 0:
            #     for ID in position.long.tradeIDs:
            #         trade_IDs.append(ID)
            # if abs(pos["short_units"]) > 0:
            #     for ID in position.short.tradeIDs:
            #         trade_IDs.append(ID)

            if instrument is not None and position.instrument == instrument:
                open_positions[position.instrument] = Position(**pos)
            elif instrument is None:
                open_positions[position.instrument] = Position(**pos)

        return open_positions

    def get_candles(
            self,
            instrument: str,
            granularity: str = None,
            count: int = None,
            start_time: datetime = None,
            end_time: datetime = None,
            *args,
            **kwargs,
    ) -> pd.DataFrame:
        """Get the historical OHLCV candles for an instrument."""
        """Retrieves historical price data of a instrument from Oanda v20 API.

        Parameters
        ----------
        instrument : str
            The instrument to fetch data for.

        granularity : str
            The candlestick granularity, specified as a TimeDelta string
            (eg. '30s', '5min' or '1d').

        count : int, optional
            The number of candles to fetch (maximum 5000). The default is None.

        start_time : datetime, optional
            The data start time. The default is None.

        end_time : datetime, optional
            The data end time. The default is None.
            
        Returns
        -------
        data : DataFrame
            The price data, as an OHLC DataFrame.

        Notes
        -----
            If candlestick cound is provided and either of the start time or end time is not provided, the count is 
            treated as DAYS to calculate the start and end time to fetch the candles.
            
            If a candlestick count is provided, only one of start time or end
            time should be provided. If neither is provided, the N most
            recent candles will be provided. If both are provided, the count
            will be ignored, and instead the dates will be used.
        """

        self._check_connection()
        instrument = str(self.nseData[self.nseData['TradingSymbol'] == instrument]['Token'].values[0])

        granularity = pd.Timedelta(granularity).total_seconds()
        if granularity < 60:
            throw_error("Minimum timeframe is 1Min (60s). Supported timeframes are [1, 3, 5, 10, 15, 30, 60, 120, 240] in minutes")
        elif granularity >60:
            granularity = str(int(granularity/60))

        self._logger.debug(f"Getting candle data for {instrument} with timeframe = {granularity} ")

        if count is not None:
            # either of count, start_time+count, end_time+count (or start_time+end_time+count)
            # if count is provided, count must be less than 5000
            if start_time is None and end_time is None:
                end_time = datetime.now()
                start_time = end_time - timedelta(days=count)
                response = self.api.get_time_price_series(exchange='NSE',

                                                          token=instrument, starttime=start_time.timestamp(),
                                                          endtime=end_time.timestamp(), interval=granularity)
                data = self._response_to_df(response)
            elif start_time is not None and end_time is None:
                # start_time + count
                from_time = start_time.timestamp()
                response = self.api.get_time_price_series(exchange='NSE', token=instrument, starttime=from_time, interval=granularity)
                data = self._response_to_df(response)

            elif end_time is not None and start_time is None:
                # end_time + count
                to_time = end_time.timestamp()
                from_time = end_time - timedelta(days=count)
                response = self.api.get_time_price_series(exchange='NSE', token=instrument, starttime=from_time.timestamp(),
                                                          endtime=to_time, interval=granularity)
                data = self._response_to_df(response)

            else:
                from_time = start_time.timestamp()
                to_time = end_time.timestamp()
                # try to get data
                response = self.api.get_time_price_series(exchange='NSE', token=instrument,
                                                          starttime=from_time,
                                                          endtime=to_time, interval=granularity)

                # If the request is rejected, max candles likely exceeded
                if response['stat'] == 'Not_Ok':
                    throw_error("Unable to fetch the candle data")
                else:
                    data = self._response_to_df(response)

        else:
            # count is None
            # Assume that both start_time and end_time have been specified.
            from_time = start_time.timestamp()
            to_time = end_time.timestamp()

            self._logger.debug(f"Getting candle data for {instrument} timeframe = {granularity} from = {start_time} to = {end_time}")
            # try to get data
            response = self.api.get_time_price_series( exchange='NSE', token=str(instrument),
                                                      starttime=from_time,
                                                      endtime=to_time, interval=granularity)

            # If the request is rejected, max candles likely exceeded
            if response is None or response['stat'] == 'Not_Ok':
                throw_error("Unable to fetch the candle data")
            else:
                data = self._response_to_df(response)

        return data

    def get_orderbook(self, instrument: str, *args, **kwargs):
        """todo: To implement get_quotes() method for the instrument.
        We need to call get_quotes method to fetch the quote details buy and sell bids for the instrument.
        """
        instrument = self.nseData[self.nseData['TradingSymbol'] == instrument]['Token'].values[0]
        response = self.api.get_quotes( exchange='NSE', token=instrument)
        if response['stat'] != 'Ok':
            return throw_error(f'Error Receiving quotes for instrument {instrument}')

        orderbook = {}
        for i in range(1, 5):
            orderbook["bids"].append({"price": response[f'bp{i}'], "size":response[f'bq{i}']})
            orderbook["asks"].append({"price": response[f'sp{i}'], "size":response[f'sq{i}']})

        return orderbook

    def get_public_trades(self, *args, **kwargs):
        """Get the public trade history for an instrument."""
        raise NotImplementedError

    def _response_to_df(self, response):
        """Function to convert api response into a pandas dataframe."""
        try:
            candles = pd.DataFrame.from_dict(response)
        except KeyError:
            raise Exception(
                "Error dowloading data - please check instrument"
                + " format and try again."
            )

        dataframe = pd.DataFrame(
            {
                "Open": candles['into'],
                "High": candles['inth'],
                "Low": candles['intl'],
                "Close": candles['intc'],
                "Volume": candles['v'],
            }
        )
        dataframe.index = pd.to_datetime(candles['time'])
        dataframe.drop_duplicates(inplace=True)

        return dataframe

    def get_isolated_positions(self, instrument: str = None, **kwargs):
        #todo: Implement isolated positions
        """Returns isolated positions for the specified instrument.
        #
        # Parameters
        # ----------
        # instrument : str, optional
        #     The instrument to fetch trades under. The default is None.
        # """
        # self._check_connection()
        # response = self.api.trade.list_open(accountID=self.ACCOUNT_ID)
        # oanda_open_trades = response.body["trades"]
        #
        # open_trades = {}
        # for trade in oanda_open_trades:
        #     new_trade = self._oanda_trade_to_dict(trade)
        #
        #     # Filter by instrument
        #     if instrument is not None and trade.instrument in instrument:
        #         open_trades[trade.id] = IsolatedPosition(**new_trade)
        #     elif instrument is None:
        #         open_trades[trade.id] = IsolatedPosition(**new_trade)
        #
        # return open_trades
        return NotImplementedError

    def check_trade_size(self, instrument: str, units: float) -> int:
        """
        For Stocks, you can even buy 1 stock
        """
        # minimum_units = response.body['instruments'][0].minimumTradeSize
        # trade_unit_precision = response.body["instruments"][0].tradeUnitsPrecision
        return round(units)

    def get_historical_data(self, instrument, interval, from_time, to_time):
        self._check_connection()
        return self.get_candles(instrument=instrument, granularity=interval, start_time=from_time, end_time=to_time)

    def get_pip_location(self, instrument: str):
        """Returns the pip location of the requested instrument.
        Since we are dealing in Stocks (for now), let the pip be the tick size * 5
        * 5 -> So we don't put a small stop loss
        """
        response = self.nseData[self.nseData['TradingSymbol'] == instrument]['TickSize'].values[0]
        return response

    def _check_connection(self) -> None:
        """Connects to Finvasia API. An initial call is performed to check
        for a timeout error.
        """

        # if we are already logged In
        if self.isLoggedIn:
            return

        for atempt in range(10):
            try:
                # Attempt basic task to check connection
                import pyotp
                self._logger.info("Attempting to login using specified credentials")
                login = self.api.login( userid=self.userName, password=self.password,
                                       twoFA=pyotp.TOTP(self.totpKey, interval=self.totpInterval).now(),
                                       vendor_code=self.vendorCode, api_secret=self.apiKey, imei=self.imei)
                self.isLoggedIn = login['stat'] == "Ok"
                self._logger.info(f'Login result = {self.isLoggedIn}')
                if self.isLoggedIn:
                    self._read_nse_master()

                return

            except BaseException:
                self._logger.error("Unable to login, please verify credentials")
                time.sleep(3)
            else:
                break

    def _place_market_order(self, order: Order):
        """Places market order."""
        self._check_connection()
        stop_loss_order = self._get_stop_loss_order(order)
        take_profit_details = self._get_take_profit_details(order)

        # Check position size
        size = self.check_trade_size(order.instrument, order.size)
        response = self.api.order.market(
            accountID=self.ACCOUNT_ID,
            instrument=order.instrument,
            units=order.direction * size,
            takeProfitOnFill=take_profit_details,
            **stop_loss_order,
        )
        return response

    def _place_stop_limit_order(self, order):
        """Places MarketIfTouchedOrder with Oanda.
        https://developer.oanda.com/rest-live-v20/order-df/
        """
        # TODO - this submits market if touched, options below
        ordertype = "MARKET_IF_TOUCHED"  # 'MARKET_IF_TOUCHED' # 'STOP', 'LIMIT'
        self._check_connection()

        stop_loss_order = self._get_stop_loss_order(order)
        take_profit_details = self._get_take_profit_details(order)

        # Check and correct order stop price
        price = self._check_precision(order.instrument, order.order_stop_price)
        trigger_condition = order.trigger_price
        size = self.check_trade_size(order.instrument, order.size)

        # Need to test cases when no stop/take is provided (as None type)
        response = self.api.order.market_if_touched(
            accountID=self.ACCOUNT_ID,
            instrument=order.instrument,
            units=order.direction * size,
            price=str(price),
            type=ordertype,
            takeProfitOnFill=take_profit_details,
            triggerCondition=trigger_condition,
            **stop_loss_order,
        )
        return response

    def _place_stop_order(self, order: Order):
        """Places a stop order."""
        # TODO - implement this method
        self._check_connection()

        stop_loss_order = self._get_stop_loss_order(order)
        take_profit_details = self._get_take_profit_details(order)

        # Check and correct order stop price
        price = self._check_precision(order.instrument, order.order_stop_price)
        price_bound = self._check_precision(order.instrument, order.order_stop_price)

        trigger_condition = order.trigger_price
        size = self.check_trade_size(order.instrument, order.size)

        response = self.api.order.stop(
            accountID=self.ACCOUNT_ID,
            instrument=order.instrument,
            units=order.direction * size,
            price=str(price),
            priceBound=str(price_bound),
            triggerCondition=trigger_condition,
            takeProfitOnFill=take_profit_details,
            **stop_loss_order,
        )
        return response

    def _place_limit_order(self, order: Order):
        """PLaces a limit order."""
        self._check_connection()

        stop_loss_order = self._get_stop_loss_order(order)
        take_profit_details = self._get_take_profit_details(order)

        # Check and correct order stop price
        price = self._check_precision(order.instrument, order.order_limit_price)

        trigger_condition = order.trigger_price
        size = self.check_trade_size(order.instrument, order.size)

        response = self.api.order.limit(
            accountID=self.ACCOUNT_ID,
            instrument=order.instrument,
            units=order.direction * size,
            price=str(price),
            takeProfitOnFill=take_profit_details,
            triggerCondition=trigger_condition,
            **stop_loss_order,
        )
        return response

    def _modify_trade(self, order):
        """Modifies the take profit and/or stop loss of an existing trade.

        Parameters
        ----------
        order : TYPE
            DESCRIPTION.
        """
        # Get ID of trade to modify
        modify_trade_id = order.related_orders
        trade = self.api.trade.get(
            accountID=self.ACCOUNT_ID, tradeSpecifier=modify_trade_id
        ).body["trade"]

        if order.take_profit is not None:
            # Modify trade take-profit
            tpID = trade.takeProfitOrder.id

            # Cancel existing TP
            self.api.order.cancel(self.ACCOUNT_ID, tpID)

            # Create new TP
            tp_price = self._check_precision(order.instrument, order.take_profit)
            new_tp_order = self.api.order.TakeProfitOrder(
                tradeID=str(modify_trade_id), price=str(tp_price)
            )
            response = self.api.order.create(
                accountID=self.ACCOUNT_ID, order=new_tp_order
            )
            self._check_response(response)

        if order.stop_loss is not None:
            # Modify trade stop-loss
            slID = trade.stopLossOrder.id

            # Cancel existing SL
            self.api.order.cancel(self.ACCOUNT_ID, slID)

            # Create new SL
            sl_price = self._check_precision(order.instrument, order.stop_loss)
            new_sl_order = self.api.order.StopLossOrder(
                tradeID=str(modify_trade_id), price=str(sl_price)
            )
            response = self.api.order.create(
                accountID=self.ACCOUNT_ID, order=new_sl_order
            )
            self._check_response(response)

    def _get_stop_loss_order(self, order: Order) -> dict:
        """Constructs stop loss order dictionary."""
        self._check_connection()
        if order.stop_type is not None:
            price = self._check_precision(order.instrument, order.stop_loss)

            if order.stop_type == "trailing":
                # Trailing stop loss order
                SL_type = "trailingStopLossOnFill"

                # Calculate stop loss distance
                if order.stop_distance is None:
                    # Calculate stop distance from stop loss price provided
                    if order._working_price is not None:
                        working_price = order._working_price
                    else:
                        if order.order_type == "market":
                            # Get current market price
                            last = self._get_price(order.instrument)
                            working_price = (
                                last.closeoutBid
                                if order.direction < 0
                                else last.closeoutAsk
                            )
                        elif order.order_type in ["limit", "stop-limit"]:
                            working_price = order.order_limit_price
                    distance = abs(working_price - order.stop_loss)

                else:
                    # Calculate distance using provided pip distance
                    pip_value = 10 ** self.get_pip_location(order.instrument)
                    distance = abs(order.stop_distance * pip_value)

                # Construct stop loss order details
                distance = self._check_precision(order.instrument, distance)
                SL_details = {"distance": str(distance), "type": "TRAILING_STOP_LOSS"}
            else:
                SL_type = "stopLossOnFill"
                SL_details = {"price": str(price)}

            stop_loss_order = {SL_type: SL_details}

        else:
            stop_loss_order = {}

        return stop_loss_order

    def _get_take_profit_details(self, order: Order) -> dict:
        """Constructs take profit details dictionary."""
        self._check_connection()
        if order.take_profit is not None:
            price = self._check_precision(order.instrument, order.take_profit)
            take_profit_details = {"price": str(price)}
        else:
            take_profit_details = None

        return take_profit_details

    def _check_response(self, response):
        """Checks API response (currently only for placing orders)."""
        if response.status != 201:
            message = response.body["errorMessage"]
        else:
            message = "Success."

        output = {"Status": response.status, "Message": message}
        # TODO - print errors
        return output

    def _close_position(self, instrument, long_units=None, short_units=None, **kwargs):
        """Closes all open positions on an instrument."""
        self._check_connection()
        # Check if the position is long or short
        # Temp code to close all positions
        # Close all long units
        response = self.api.position.close(
            accountID=self.ACCOUNT_ID, instrument=instrument, longUnits="ALL"
        )

        # Close all short units
        response = self.api.position.close(
            accountID=self.ACCOUNT_ID, instrument=instrument, shortUnits="ALL"
        )

        # TODO - the code below makes no sense currently; specifically,
        # position.long.Units ????

        # open_position = self.get_open_positions(instrument)

        # if len(open_position) > 0:
        #     position = open_position['position']

        #     if long_units is None:
        #         long_units  = position.long.units
        #     if short_units is None:
        #         short_units = position.short.units

        #     if long_units > 0:
        #         response = self.api.position.close(accountID=self.ACCOUNT_ID,
        #                                            instrument=instrument,
        #                                            longUnits="ALL")

        #     elif short_units > 0:
        #         response = self.api.position.close(accountID=self.ACCOUNT_ID,
        #                                            instrument=instrument,
        #                                            shortUnits="ALL")

        #     else:
        #         print("There is no current position with {} to close.".format(instrument))
        #         response = None
        # else:
        #     response = None

        return response

    def _get_precision(self, instrument: str):
        """Returns the allowable precision for a given pair."""
        # self._check_connection()
        # response = self.api.account.instruments(
        #     accountID=self.ACCOUNT_ID, instruments=instrument
        # )
        # precision = response.body["instruments"][0].displayPrecision
        return 0

    def _check_precision(self, instrument, price):
        """Modify a price based on required ordering precision for pair."""
        N = self._get_precision(instrument)
        corrected_price = round(price, N)
        return corrected_price

    def _get_order_book(self, instrument: str):
        """Returns the order book of the instrument specified."""
        return self.get_orderbook(instrument)

    def _get_position_book(self, instrument: str):
        """Returns the position book of the instrument specified."""
        response = self.api.instrument.position_book(instrument)
        return response.body["positionBook"]

    def _get_price(self, instrument: str):
        """Returns the current price of the instrument."""
        response = self.api.pricing.get(
            accountID=self.ACCOUNT_ID, instruments=instrument
        )
        return response.body["prices"][0]

    @staticmethod
    def response_to_df(response: pd.DataFrame):
        """Function to convert api response into a pandas dataframe."""
        candles = response.body["candles"]
        times = []
        close_price, high_price, low_price, open_price = [], [], [], []

        for candle in candles:
            times.append(candle.time)
            close_price.append(float(candle.mid.c))
            high_price.append(float(candle.mid.h))
            low_price.append(float(candle.mid.l))
            open_price.append(float(candle.mid.o))

        dataframe = pd.DataFrame(
            {
                "Open": open_price,
                "High": high_price,
                "Low": low_price,
                "Close": close_price,
            }
        )
        dataframe.index = pd.to_datetime(times)

        return dataframe

    @staticmethod
    def finvasia_res_to_order(orig_order: Order, fin_response) -> dict:
        orig_order.id = fin_response['norenordno']
        orig_order.status = "open" if fin_response['stat'] == 'Ok' else "cancelled"
        return orig_order.as_dict()

    @staticmethod
    def order_to_finvasia_order(order: Order) -> dict:
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

    @staticmethod
    def finvasia_to_native_order(finvasia_order) -> Order:
        direction = 1 if finvasia_order['trantype'] == "B" else -1
        price_type = finvasia_order['prctyp']
        order_type = 'market'
        status = "pending"
        if finvasia_order['status'] == "REJECT":
            status = "cancelled"

        limitprice = None
        orderPrice = None

        if price_type == 'LMT':
            order_type = 'limit'
            limitprice = finvasia_order['prc']
        elif price_type == 'SL-LMT':
            order_type = 'stop-limit'
            limitprice = finvasia_order['prc']
        elif price_type == 'SL-MKT':
            order_type = 'stop'
            orderPrice = finvasia_order['prc']
        else:
            orderPrice = finvasia_order['prc']

        return Order(id=finvasia_order['norenorderno'],
                     direction=direction,
                     instrument=finvasia_order['tsym'],
                     order_type=order_type,
                     size=float(finvasia_order['qty']),
                     order_stop_price=orderPrice,
                     order_limit_price=limitprice,
                     status=status)

    @staticmethod
    def finvasia_trades_to_trades(finvasiaOrder) -> Trade:
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

    def _read_nse_master(self):
        from datetime import date
        from pathlib import Path
        import requests
        from zipfile import ZipFile
        from io import BytesIO

        self._logger.info("Reading NSE Master")
        filename = f'NSE_{str(date.today())}.txt'
        if Path(filename).is_file():
            self.nseData = pd.read_csv(filename)
        else:
            r = requests.get("https://api.shoonya.com/NSE_symbols.txt.zip")
            files = ZipFile(BytesIO(r.content))
            self.nseData = pd.read_csv(files.open("NSE_symbols.txt"))
            self.nseData.to_csv(filename, index=False, header=True)

        # removed unnamed
        self.nseData = self.nseData.loc[:, ~self.nseData.columns.str.contains('^Unnamed')]
        # Finvasia packages some TEST symbols in the master data, exclude them as well.
        self.nseData = self.nseData[~self.nseData.Symbol.str.contains("NSETEST")]
        # Get only EQ or Index
        self.nseData = self.nseData[self.nseData['Instrument'].isin(['EQ', 'INDEX'])]
        self._logger.info("NSE Master read -> ")
        self._logger.info("---------------------------------NSE MASTER START-----------------------------------------")
        self._logger.info(self.nseData.head())
        self._logger.info("---------------------------------NSE MASTER END-------------------------------------------")


    def _get_my_orders(self, status:str) -> dict:
        self._check_connection()
        response = self.api.get_order_book()
        orders = {}
        for order in response:
            native_order = self.finvasia_to_native_order(order)
            orders[native_order.id] = native_order
        final_orders = {k: v for k, v in orders.items() if v.status == status }
        return final_orders