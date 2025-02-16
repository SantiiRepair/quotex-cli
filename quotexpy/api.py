"""Module for Quotex websocket"""

import os
import ssl
import time
import json
import pickle
import typing
import logging
import urllib3
import requests
import threading


from quotexpy.http.login import Login
from quotexpy.http.logout import Logout
from quotexpy.http.refresh import Refresh
from quotexpy.ws.channels.ssid import Ssid
from quotexpy.ws.channels.trade import Trade
from quotexpy.exceptions import QuotexTimeout
from quotexpy.ws.client import WebsocketClient
from quotexpy.ws.objects.candles import Candles
from quotexpy.ws.objects.profile import Profile
from quotexpy.ws.objects.timesync import TimeSync
from quotexpy.ws.channels.candles import GetCandles
from quotexpy.ws.channels.sell_option import SellOption
from quotexpy.ws.objects.listinfodata import ListInfoData
from quotexpy.utils import (
    nested_dict,
    sessions_file_path,
    cert_path,
)

urllib3.disable_warnings()
logger = logging.getLogger(__name__)

if not os.path.exists(cert_path):
    url = "https://raw.githubusercontent.com/SantiiRepair/quotexpy/main/.github/fs/cacert.pem"
    response = requests.get(url)
    if response.status_code == 200:
        with open(cert_path, "wb") as f:
            f.write(response.content)

os.environ["SSL_CERT_FILE"] = cert_path
os.environ["WEBSOCKET_CLIENT_CA_BUNDLE"] = cert_path
cacert = os.environ.get("WEBSOCKET_CLIENT_CA_BUNDLE")


class QuotexAPI(object):
    """Class for communication with Quotex API"""

    socket_option_opened = {}
    trade_id = None
    trace_ws = False
    buy_expiration = None
    current_asset = None
    trade_successful = None
    account_balance = None
    last_operation = {}
    account_type = None
    instruments = None
    training_balance_edit_request = None
    profit_in_operation = None
    sold_options_respond = None
    sold_digital_options_respond = None
    listinfodata = ListInfoData()
    timesync = TimeSync()
    candles = Candles()
    history = []

    SSID = None
    wss_message = None
    check_websocket_if_connect = None
    ssl_Mutual_exclusion = False
    ssl_Mutual_exclusion_write = False
    started_listen_instruments = True
    check_rejected_connection = False
    check_accepted_connection = False
    check_websocket_if_error = False
    websocket_error_reason = None
    balance_id = None
    settings = None

    def __init__(self, email="", password="", **kwargs):
        """
        :param email: The email of a Quotex account.
        :param password: The password of a Quotex account.
        """
        self.email = email
        self.password = password

        self.kwargs = kwargs
        self._temp_status = ""

        self.cookies = None
        self.profile = None
        self.settings_list = {}
        self.candle_v2_data = {}
        self.get_candle_data = {}
        self.websocket_thread = None
        self.websocket_client = None
        self.SSID = kwargs.get("ssid", None)
        self.time_period = kwargs.get("time_period", 60)
        self.current_asset = kwargs.get("current_asset", None)
        self.signal_data = nested_dict(2, dict)

        self.user_agent = None
        self.profile = Profile()
        self.realtime_price = {}
        self.realtime_sentiment = {}
        self.wss_url = "wss://ws2.qxbroker.com/socket.io/?EIO=3&transport=websocket"

        self.logger = logging.getLogger(__name__)

    @property
    def websocket(self):
        """Property to get websocket.

        :returns: The instance of :class:`WebSocket <websocket.WebSocket>`.
        """
        return self.websocket_client.wss

    @property
    def refresh(self):
        """Property for Quotex http refresh ssid resource.
        :returns: The instance of :class:`Refresh
            <quotexpy.http.refresh.Refresh>`.
        """
        return Refresh(self)

    @property
    def logout(self):
        """Property for get Quotex http login resource.
        :returns: The instance of :class:`Login
            <quotexpy.http.login.Login>`.
        """
        return Logout(self)

    @property
    def login(self):
        """Property for get Quotex http login resource.
        :returns: The instance of :class:`Login
            <quotexpy.http.login.Login>`.
        """
        return Login(self)

    @property
    def ssid(self):
        """Property for get Quotex websocket ssid channel.
        :returns: The instance of :class:`Ssid
            <quotexpy.ws.channels.ssid.Ssid>`.
        """
        return Ssid(self)

    @property
    def trade(self):
        """Property for get Quotex websocket ssid channel.
        :returns: The instance of :class:`Buy
            <Quotex.ws.channels.buy.Buy>`.
        """
        return Trade(self)

    @property
    def sell_option(self):
        return SellOption(self)

    @property
    def get_candles(self):
        """Property for get Quotex websocket candles channel.

        :returns: The instance of :class:`GetCandles
            <quotexpy.ws.channels.candles.GetCandles>`.
        """
        return GetCandles(self)

    async def connect(self) -> bool:
        """Method for connection to Quotex API"""
        self.ssl_Mutual_exclusion = False
        self.ssl_Mutual_exclusion_write = False
        if self.check_websocket_if_connect:
            self.close()
        if not self.SSID:
            self.SSID, self.cookies = await self.get_ssid()
        check_websocket = self.start_websocket()
        return check_websocket

    async def get_ssid(self) -> typing.Tuple[str, str]:
        ssid, cookies = self.check_session()
        if not ssid:
            self.logger.info("authenticating user")
            ssid, cookies = self.login(self.email, self.password, **self.kwargs)
        return ssid, cookies

    def check_session(self) -> typing.Tuple[str, str]:
        data = {}
        if os.path.isfile(sessions_file_path):
            with open(sessions_file_path, "rb") as file:
                data: dict = pickle.load(file)

            sessions: list[dict] = data.get(self.email, [])
            for session in sessions:
                return session.get("ssid", ""), session.get("cookies", "")
        return "", ""

    def get_candle_v2(self) -> None:
        payload = {"_placeholder": True, "num": 0}
        data = f'451-["history/list/v2", {json.dumps(payload)}]'
        self.send_websocket_request(data)

    def subscribe_realtime_candle(self, asset: str, period: int) -> None:
        self.realtime_price[asset] = []
        payload = {"asset": asset, "period": period}
        data = f'42["instruments/update", {json.dumps(payload)}]'
        self.send_websocket_request(data)
        data = f'42["depth/follow", "{asset}"]'
        self.send_websocket_request(data)
        payload = {"asset": asset, "version": "1.0.0"}
        data = f'42["chart_notification/get", {json.dumps(payload)}]'
        self.send_websocket_request(data)
        self.send_websocket_request('42["tick"]')

    def unsubscribe_realtime_candle(self, asset) -> None:
        data = f'42["subfor", {json.dumps(asset)}]'
        self.send_websocket_request(data)

    def send_websocket_request(self, data: str, sync=True, no_force_send=True) -> None:
        """Send websocket request to Quotex server.
        :param data: The websocket request data.
        :param no_force_send: Default True.
        """
        if self.check_websocket_if_connect == 0:
            self.logger.info("websocket connection closed")
            return

        while (self.ssl_Mutual_exclusion or self.ssl_Mutual_exclusion_write) and no_force_send:
            pass

        self.ssl_Mutual_exclusion_write = True

        self.logger.debug(data)
        self.websocket.send(data)

        if sync:
            self.websocket.send('42["tick"]')
            self.websocket.send('42["indicator/list"]')
            self.websocket.send('42["drawing/load"]')
            self.websocket.send('42["pending/list"]')
            self.websocket.send('42["chart_notification/get"]')

            if self.current_asset:
                payload = json.dumps({"asset": self.current_asset, "period": self.time_period})
                self.websocket.send(f'42["instruments/update",{payload}]')
                self.websocket.send(f'42["depth/follow","{self.current_asset}"]')

        self.ssl_Mutual_exclusion_write = False

    def edit_training_balance(self, amount) -> None:
        data = f'42["demo/refill",{json.dumps(amount)}]'
        self.send_websocket_request(data)

    def start_websocket(self) -> bool:
        self.check_websocket_if_connect = None
        self.check_websocket_if_error = False
        self.websocket_error_reason = None
        self.websocket_client = WebsocketClient(self)

        self.websocket_thread = threading.Thread(
            target=self.websocket.run_forever,
            kwargs={
                "suppress_origin": True,
                "ping_interval": 25000,
                "ping_timeout": 5000,
                "ping_payload": "2",
                "origin": "https://qxbroker.com",
                "host": "ws2.qxbroker.com",
                "sslopt": {
                    "cert_reqs": ssl.CERT_NONE,
                    "ca_certs": cacert,
                    "ssl_version": ssl.PROTOCOL_TLSv1_2,
                },
            },
        )

        self.websocket_thread.daemon = True
        self.websocket_thread.start()

        while True:
            try:
                if self.check_websocket_if_error:
                    self.logger.error(self.websocket_error_reason)
                    return False
                if self.check_websocket_if_connect == 0:
                    self.logger.debug("websocket connection closed")
                    return False
                if self.check_websocket_if_connect == 1:
                    self.logger.debug("websocket successfully connected")
                    return True
            except:
                pass

    def send_ssid(self, max_attemps=10) -> bool:
        """
        Send ssid to Quotex
            max_attemps - time to wait for authorization in seconds
        """
        self.profile.msg = None
        if not self.SSID:
            if os.path.exists(sessions_file_path):
                os.remove(sessions_file_path)
            return False

        self.ssid(self.SSID)
        start_time = time.time()
        previous_second = -1

        while not self.account_balance:
            elapsed_time = time.time() - start_time
            current_second = int(elapsed_time)
            if current_second != previous_second:
                previous_second = current_second
            if elapsed_time >= max_attemps:
                raise QuotexTimeout(f"sending authorization with SSID {self.SSID} took too long to respond")
        return True

    def close(self) -> None:
        if self.websocket_client:
            self.websocket.close()
            self.websocket_thread.join()

    def websocket_alive(self) -> bool:
        return self.websocket_thread.is_alive()
