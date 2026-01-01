import json
from enum import Enum
from typing import Optional, Final
from core.logger import server_logger
from core.message import JsonMessage
from core.network import ConnectionStub
from core.server import Server, ServerInfo
import threading
from collections import defaultdict


class RequestType(Enum):
    SET = 1
    GET = 2
    VER_GET = 3


class KVGetRequest:
    def __init__(self, msg: JsonMessage):
        self._json_message = msg
        assert "key" in self._json_message, self._json_message

    @property
    def key(self) -> str:
        return self._json_message["key"]

    @property
    def json_msg(self) -> JsonMessage:
        return self._json_message


class KVSetRequest:
    def __init__(self, msg: JsonMessage):
        self._json_message = msg
        assert "key" in self._json_message, self._json_message
        assert "val" in self._json_message, self._json_message

    @property
    def key(self) -> str:
        return self._json_message["key"]

    @property
    def val(self) -> str:
        return self._json_message["val"]

    @property
    def version(self) -> Optional[int]:
        return self._json_message.get("ver")

    @version.setter
    def version(self, ver: int) -> None:
        self._json_message["ver"] = ver

    @property
    def json_msg(self) -> JsonMessage:
        return self._json_message

    def __str__(self) -> str:
        return str(self._json_message)


class KVVerGetRequest:
    def __init__(self, msg: JsonMessage):
        self._json_message = msg
        assert "key" in self._json_message, self._json_message

    @property
    def key(self) -> str:
        return self._json_message["key"]

    @property
    def json_msg(self) -> JsonMessage:
        return self._json_message


class CRAQServer(Server):

    def __init__(
        self,
        info: ServerInfo,
        connection_stub: ConnectionStub,
        next: Optional[ServerInfo],
        prev: Optional[ServerInfo],
        tail: ServerInfo,
    ) -> None:
        super().__init__(info, connection_stub)
        self.next: Final[Optional[str]] = None if next is None else next.name
        self.prev: Final[Optional[str]] = prev if prev is None else prev.name
        self.tail: Final[str] = tail.name
        self.d: dict[str, dict] = {}
        self.lock = threading.Lock()
        self.locks = defaultdict(threading.Lock)  # One lock per server

    def _process_req(self, msg: JsonMessage) -> JsonMessage:
        if msg.get("type") == RequestType.GET.name:
            return self._get(KVGetRequest(msg))
        elif msg.get("type") == RequestType.SET.name:
            return self._set(KVSetRequest(msg))
        elif msg.get("type") == RequestType.VER_GET.name:
            return self._ver_get(KVVerGetRequest(msg))
        else:
            server_logger.critical("Invalid message type")
            return JsonMessage({"status": "Unexpected type"})

    def _get(self, req: KVGetRequest) -> JsonMessage:
        if req.key in self.d and not self.d[req.key]["dirty"]:
            version = self.d[req.key]["maxV"]
            value = self.d[req.key][version]
            return JsonMessage({"status": "OK", "val": value})
        else:
            ver_req_msg = JsonMessage({"type": "VER_GET", "key": req.key})
            flag = True
            while flag:
                try:
                    ver_response = self._connection_stub.send(
                        from_=self._info.name, to=self.tail, message=ver_req_msg
                    )
                    flag = False
                except:
                    continue

            with self.locks[self._info.name]:
                maxV = self.d[req.key]["maxV"]
                version = ver_response["ver"]
                value = self.d[req.key][version]

                if version == maxV:
                    self.d[req.key]["dirty"] = False

            return JsonMessage({"status": "OK", "val": value})

    def _set(self, req: KVSetRequest) -> JsonMessage:
        _logger = server_logger.bind(server_name=self._info.name)
        _logger.debug(f"Setting {req.key} to {req.val}")

        with self.locks[self._info.name]:
            if self.next is not None:  # non-tail
                if self._info.name == "a":  # head
                    if req.key not in self.d:
                        version = 1
                        self.d[req.key] = {
                            version: req.val,
                            "maxV": version,
                            "dirty": False,
                        }
                    else:
                        version = self.d[req.key]["maxV"] + 1
                        self.d[req.key].update(
                            {version: req.val, "maxV": version, "dirty": True}
                        )

                    req.version = version

                else:  # b, c
                    if req.key not in self.d:
                        self.d[req.key] = {
                            req.version: req.val,
                            "maxV": req.version,
                            "dirty": False,
                        }
                    else:
                        self.d[req.key].update(
                            {req.version: req.val, "maxV": req.version, "dirty": True}
                        )

                response = self._connection_stub.send(
                    from_=self._info.name, to=self.next, message=req.json_msg
                )

                maxV = self.d[req.key]["maxV"]
                value = self.d[req.key][maxV]
                if response["ver"] == maxV:
                    self.d[req.key]["dirty"] = False

                return response
            else:  # tail
                self.d[req.key] = {
                    req.version: req.val,
                    "maxV": req.version,
                    "dirty": False,
                }
                return JsonMessage({"status": "OK", "ver": req.version})

    def _ver_get(self, req: KVVerGetRequest) -> JsonMessage:
        if req.key in self.d:
            version = self.d[req.key]["maxV"]
            return JsonMessage({"key": req.key, "ver": version})
        return JsonMessage({"status": "Error", "message": "Key not found"})
