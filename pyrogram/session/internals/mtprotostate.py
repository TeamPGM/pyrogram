import logging
import os
import struct
import time
from collections import deque
from typing import Any

from pyrogram.raw.core import GzipPacked, TLObject, Message
from pyrogram.raw.functions import InvokeAfterMsg

# N is not  specified in https://core.telegram.org/mtproto/security_guidelines#checking-msg-id, but 500 is reasonable
MAX_RECENT_MSG_IDS = 500

MSG_TOO_NEW_DELTA = 30
MSG_TOO_OLD_DELTA = 300

# Something must be wrong if we ignore too many messages at the same time
MAX_CONSECUTIVE_IGNORED = 10


class _OpaqueRequest(TLObject):
    """
    Wraps a serialized request into a type that can be serialized again.
    """
    def __init__(self, data: bytes):
        self.data = data

    def write(self, *args: Any) -> bytes:
        return self.data


class MTProtoState:
    """
    `telethon.network.mtprotosender.MTProtoSender` needs to hold a state
    in order to be able to encrypt and decrypt incoming/outgoing messages,
    as well as generating the message IDs. Instances of this class hold
    together all the required information.

    It doesn't make sense to use `telethon.sessions.abstract.Session` for
    the sender because the sender should *not* be concerned about storing
    this information to disk, as one may create as many senders as they
    desire to any other data center, or some CDN. Using the same session
    for all these is not a good idea as each need their own authkey, and
    the concept of "copying" sessions with the unnecessary entities or
    updates state for these connections doesn't make sense.

    While it would be possible to have a `MTProtoPlainState` that does no
    encryption so that it was usable through the `MTProtoLayer` and thus
    avoid the need for a `MTProtoPlainSender`, the `MTProtoLayer` is more
    focused to efficiency and this state is also more advanced (since it
    supports gzipping and invoking after other message IDs). There are too
    many methods that would be needed to make it convenient to use for the
    authentication process, at which point the `MTProtoPlainSender` is better.
    """
    def __init__(self):
        self._log = logging.getLogger(__name__)
        self.time_offset = 0
        self.salt = 0

        self.id = self._sequence = self._last_msg_id = None
        self._recent_remote_ids = deque(maxlen=MAX_RECENT_MSG_IDS)
        self._highest_remote_id = 0
        self._ignore_count = 0
        self.reset()

    def reset(self):
        """
        Resets the state.
        """
        # Session IDs can be random on every connection
        self.id = struct.unpack('q', os.urandom(8))[0]
        self._sequence = 0
        self._last_msg_id = 0
        self._recent_remote_ids.clear()
        self._highest_remote_id = 0
        self._ignore_count = 0

    def write_data_as_message(self, data: "TLObject", content_related: bool, *, after_id=None) -> Message:
        """
        Writes a message containing the given data into buffer.

        Returns the message id.
        """
        msg_id = self._get_new_msg_id()
        seq_no = self._get_seq_no(content_related)
        if after_id is None:
            body = GzipPacked.gzip_if_smaller(content_related, data)
        else:
            # The `RequestState` stores `bytes(request)`, not the request itself.
            # `invokeAfterMsg` wants a `TLRequest` though, hence the wrapping.
            body = GzipPacked.gzip_if_smaller(
                content_related,
                InvokeAfterMsg(msg_id=after_id, query=data),
            )
        return Message(msg_id=msg_id, seq_no=seq_no, length=len(body), body=body)

    def _get_new_msg_id(self):
        """
        Generates a new unique message ID based on the current
        time (in ms) since epoch, applying a known time offset.
        """
        now = time.time() + self.time_offset
        nanoseconds = int((now - int(now)) * 1e+9)
        new_msg_id = (int(now) << 32) | (nanoseconds << 2)

        if self._last_msg_id >= new_msg_id:
            new_msg_id = self._last_msg_id + 4

        self._last_msg_id = new_msg_id
        return new_msg_id

    def _get_seq_no(self, content_related):
        """
        Generates the next sequence number depending on whether
        it should be for a content-related query or not.
        """
        if content_related:
            result = self._sequence * 2 + 1
            self._sequence += 1
            return result
        else:
            return self._sequence * 2
