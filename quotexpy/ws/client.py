"""Module for Quotex websocket."""

import os
import json
import time
import base64
import random
import logging

import websocket
from quotexpy.http.user_agents import agents
from quotexpy.utils import asrun, is_valid_json, sessions_file_path

user_agent_list = agents.split("\n")


class WebsocketClient(object):
    """Class for work with Quotex API websocket."""

    def generate_websocket_key(self):
        random_bytes = os.urandom(16)
        websocket_key = base64.b64encode(random_bytes).decode("utf-8")
        return websocket_key

    def __init__(self, api):
        """
        :param api: The instance of :class:`QuotexAPI
            <quotexpy.api.QuotexAPI>`.
        :trace_ws: Enables and disable `enableTrace` in WebSocket Client.
        """
        self.api = api

        self.headers = {
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Accept-Language": "en-US,en;q=0.9,es-ES;q=0.8,es;q=0.7",
            "Cache-Control": "no-cache",
            "Connection": "Upgrade",
            "Host": "ws2.qxbroker.com",
            "Origin": "https://qxbroker.com",
            "Pragma": "no-cache",
            "Sec-WebSocket-Version": "13",
            "Upgrade": "websocket",
            "Sec-WebSocket-Key": self.generate_websocket_key(),
            "User-Agent": (
                self.api.user_agent
                if self.api.user_agent
                else user_agent_list[random.randint(0, len(user_agent_list) - 1)]
            ),
        }

        websocket.enableTrace(self.api.trace_ws)
        self.wss = websocket.WebSocketApp(
            self.api.wss_url,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close,
            on_open=self.on_open,
            on_ping=self.on_ping,
            on_pong=self.on_pong,
            header=self.headers,
            cookie=self.api.cookies,
        )

        self.logger = logging.getLogger(__name__)

    def on_message(self, wss, wm):
        """Method to process websocket messages."""
        self.api.ssl_Mutual_exclusion = True
        current_time = time.localtime()
        if isinstance(wm, bytes):
            wm = wm[1:].decode()
        self.logger.info(wm)
        if current_time.tm_sec in [0, 20, 40]:
            self.wss.send('42["tick"]')
        if "authorization/reject" in wm:
            if os.path.isfile(sessions_file_path):
                os.remove(sessions_file_path)
            self.api.SSID = None
            self.api.check_rejected_connection = 1
        elif "s_authorization" in wm:
            self.api.check_accepted_connection = 1
        elif "instruments/list" in wm:
            self.api.started_listen_instruments = True
        try:
            if is_valid_json(wm):
                message = json.loads(wm)
                self.api.wss_message = message
            if isinstance(self.api.wss_message, dict):
                if self.api.wss_message.get("signals"):
                    time_in = self.api.wss_message.get("time")
                    for i in self.api.wss_message["signals"]:
                        try:
                            self.api.signal_data[i[0]] = {}
                            self.api.signal_data[i[0]][i[2]] = {}
                            self.api.signal_data[i[0]][i[2]]["dir"] = i[1][0]["signal"]
                            self.api.signal_data[i[0]][i[2]]["duration"] = i[1][0]["timeFrame"]
                        except:
                            self.api.signal_data[i[0]] = {}
                            self.api.signal_data[i[0]][time_in] = {}
                            self.api.signal_data[i[0]][time_in]["dir"] = i[1][0][1]
                            self.api.signal_data[i[0]][time_in]["duration"] = i[1][0][0]
                elif (
                    (
                        self.api.wss_message.get("accountBalance") is not None
                        and self.api.wss_message.get("isDemo") is not None
                    )
                    or (
                        self.api.wss_message.get("balance") is not None
                        and self.api.wss_message.get("isDemo") is not None
                    )
                    or (
                        self.api.wss_message.get("liveBalance") is not None
                        or self.api.wss_message.get("demoBalance") is not None
                    )
                    or self.api.wss_message.get("id") is not None
                ):
                    if (
                        self.api.wss_message.get("accountBalance") is not None
                        and self.api.wss_message.get("isDemo") is not None
                    ):
                        self.api.account_balance = {
                            "liveBalance": (
                                self.api.wss_message.get("accountBalance")
                                if self.api.wss_message.get("isDemo") != 1
                                else 0
                            ),
                            "demoBalance": (
                                self.api.wss_message.get("accountBalance")
                                if self.api.wss_message.get("isDemo") == 1
                                else 0
                            ),
                        }
                    elif (
                        self.api.wss_message.get("balance") is not None
                        and self.api.wss_message.get("isDemo") is not None
                    ):
                        self.api.training_balance_edit_request = self.api.wss_message
                        self.api.account_balance = {
                            "liveBalance": (
                                self.api.wss_message.get("balance") if self.api.wss_message.get("isDemo") != 1 else 0
                            ),
                            "demoBalance": (
                                self.api.wss_message.get("balance") if self.api.wss_message.get("isDemo") == 1 else 0
                            ),
                        }
                    elif (
                        self.api.wss_message.get("liveBalance") is not None
                        or self.api.wss_message.get("demoBalance") is not None
                    ):
                        self.api.account_balance = self.api.wss_message
                    if self.api.wss_message.get("id"):
                        self.api.trade_id = self.api.wss_message.get("id")
                        self.api.trade_successful = self.api.wss_message
                elif self.api.wss_message.get("index"):
                    self.api.candles.candles_data = self.api.wss_message
                    self.api.timesync.server_timestamp = self.api.wss_message.get("closeTimestamp")
                elif self.api.wss_message.get("ticket"):
                    self.api.sold_options_respond = self.api.wss_message
                elif all(key in self.api.wss_message for key in ["asset", "history", "candles"]):
                    asset = self.api.wss_message.get("asset")
                    history = self.api.wss_message.get("history")
                    candles: list = self.api.wss_message.get("candles")
                    self.api.history = history
                    self.api.candles.candles_data = candles
                    self.api.candle_v2_data[asset] = [
                        {
                            "time": candle[0],
                            "open": candle[1],
                            "close": candle[2],
                            "high": candle[3],
                            "low": candle[4],
                        }
                        for candle in candles
                    ]
                elif self.api.wss_message.get("error"):
                    self.api.websocket_error_reason = self.api.wss_message.get("error")
                    self.api.check_websocket_if_error = True
                    if self.api.websocket_error_reason == "not_money":
                        self.api.account_balance = {"liveBalance": 0, "demoBalance": 0}
        except Exception as err:
            self.logger.error(err)

        try:
            if self.api.wss_message and not isinstance(self.api.wss_message, int):
                if "call" in wm or "put" in wm:
                    self.api.instruments = self.api.wss_message
                if isinstance(self.api.wss_message, list):
                    for item in self.api.wss_message:
                        if "settings" in item:
                            self.api.settings = json.loads(item.get("settings"))
                        if "amount" in item and "profit" in item:
                            self.api.last_operation = self.api.wss_message[0]
                            break
                if str(self.api.wss_message) == "41":
                    self.logger.info("disconnection event triggered by the platform, running automatic reconnection")
                    self.api.check_websocket_if_connect = 0
                    asrun(self.api.connect())
                if "51-" in str(self.api.wss_message):
                    self.api._temp_status = str(self.api.wss_message)
                elif self.api._temp_status == """451-["settings/list",{"_placeholder":true,"num":0}]""":
                    self.api.settings_list = self.api.wss_message
                    self.api._temp_status = ""
                elif len(self.api.wss_message[0]) == 4:
                    result = {"time": self.api.wss_message[0][1], "price": self.api.wss_message[0][2]}
                    self.api.realtime_price[self.api.wss_message[0][0]].append(result)
                elif len(self.api.wss_message[0]) == 2:
                    result = {
                        "sentiment": {
                            "sell": 100 - int(self.api.wss_message[0][1]),
                            "buy": int(self.api.wss_message[0][1]),
                        }
                    }
                    self.api.realtime_sentiment[self.api.wss_message[0][0]] = result
        except Exception as err:
            self.logger.error(err)

        self.api.ssl_Mutual_exclusion = False

    def on_error(self, _, err):
        """Method to process websocket errors."""
        self.api.websocket_error_reason = str(err)
        self.api.check_websocket_if_error = True

    def on_open(self, _):
        """Method to process websocket open."""
        self.logger.info("from ws client, websocket client connected")
        self.api.check_websocket_if_connect = 1
        self.wss.send('42["tick"]')
        self.wss.send('42["indicator/list"]')
        self.wss.send('42["drawing/load"]')
        self.wss.send('42["pending/list"]')
        self.wss.send('42["chart_notification/get"]')

    def on_close(self, wss, close_status_code, close_msg):
        """Method to process websocket close."""
        self.logger.info("from ws client, websocket connection closed")
        self.api.check_websocket_if_connect = 0

    def on_ping(self, wss, ping_msg):
        pass

    def on_pong(self, wss, pong_msg):
        self.wss.send("2")
