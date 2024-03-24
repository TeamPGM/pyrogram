import asyncio
from typing import Optional

from pyrogram.raw.types import MsgsAck

from pyrogram.raw.core import TLObject, Message


class RequestState:
    """
    This request state holds several information relevant to sent messages,
    in particular the message ID assigned to the request, the container ID
    it belongs to, the request itself, the request as bytes, and the future
    result that will eventually be resolved.
    """
    __slots__ = ('container_id', 'msg_id', 'request', 'packed', 'future', 'after')

    def __init__(self, request: "TLObject", after=None):
        self.container_id = None
        self.msg_id = None
        self.request = request
        self.packed: Optional["Message"] = None
        self.future = asyncio.Future()
        self.after = after

    @property
    def need_wait(self) -> bool:
        if not isinstance(self.request, TLObject):
            return False
        if isinstance(self.request, MsgsAck):
            return False
        return True
