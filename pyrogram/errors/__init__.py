#  Pyrogram - Telegram MTProto API Client Library for Python
#  Copyright (C) 2017-present Dan <https://github.com/delivrance>
#
#  This file is part of Pyrogram.
#
#  Pyrogram is free software: you can redistribute it and/or modify
#  it under the terms of the GNU Lesser General Public License as published
#  by the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  Pyrogram is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU Lesser General Public License for more details.
#
#  You should have received a copy of the GNU Lesser General Public License
#  along with Pyrogram.  If not, see <http://www.gnu.org/licenses/>.
import struct

from .exceptions import *
from .rpc_error import UnknownError


class BadMsgNotification(Exception):
    descriptions = {
        16: "The msg_id is too low, the client time has to be synchronized.",
        17: "The msg_id is too high, the client time has to be synchronized.",
        18: "Incorrect two lower order of the msg_id bits, the server expects the client message "
            "msg_id to be divisible by 4.",
        19: "The container msg_id is the same as the msg_id of a previously received message.",
        20: "The message is too old, it cannot be verified by the server.",
        32: "The msg_seqno is too low.",
        33: "The msg_seqno is too high.",
        34: "An even msg_seqno was expected, but an odd one was received.",
        35: "An odd msg_seqno was expected, but an even one was received.",
        48: "Incorrect server salt.",
        64: "Invalid container."
    }

    def __init__(self, code):
        description = self.descriptions.get(code, "Unknown error code")
        super().__init__(f"[{code}] {description}")


class SecurityError(Exception):
    """Generic security error."""

    @classmethod
    def check(cls, cond: bool, msg: str):
        """Raises this exception if the condition is false"""
        if not cond:
            raise cls(f"Check failed: {msg}")


class SecurityCheckMismatch(SecurityError):
    """Raised when a security check mismatch occurs."""

    def __init__(self, msg: str = None):
        super().__init__("A security check mismatch has occurred." if msg is None else msg)


class CDNFileHashMismatch(SecurityError):
    """Raised when a CDN file hash mismatch occurs."""

    def __init__(self, msg: str = None):
        super().__init__("A CDN file hash mismatch has occurred." if msg is None else msg)


class InvalidChecksumError(SecurityCheckMismatch):
    """
    Occurs when using the TCP full mode and the checksum of a received
    packet doesn't match the expected checksum.
    """
    def __init__(self, checksum, valid_checksum):
        super().__init__(
            'Invalid checksum ({} when {} was expected). '
            'This packet should be skipped.'
            .format(checksum, valid_checksum))

        self.checksum = checksum
        self.valid_checksum = valid_checksum


class InvalidBufferError(BufferError):
    """
    Occurs when the buffer is invalid, and may contain an HTTP error code.
    For instance, 404 means "forgotten/broken authorization key", while
    """
    def __init__(self, payload):
        self.payload = payload
        if len(payload) == 4:
            self.code = -struct.unpack('<i', payload)[0]
            super().__init__(
                'Invalid response buffer (HTTP code {})'.format(self.code))
        else:
            self.code = None
            super().__init__(
                'Invalid response buffer (too short {})'.format(self.payload))
