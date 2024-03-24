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

import asyncio
import bisect
import collections
import logging
import os
from hashlib import sha1
from io import BytesIO
from typing import Dict

import pyrogram
from pyrogram import raw
from pyrogram.connection import Connection, get_connection_class, TcpMTProxy
from pyrogram.crypto import mtproto
from pyrogram.errors import (
    RPCError, InternalServerError, AuthKeyDuplicated, FloodWait, ServiceUnavailable, BadMsgNotification,
    SecurityCheckMismatch, InvalidBufferError, SecurityError
)
from pyrogram.raw.all import layer
from pyrogram.raw.core import TLObject, MsgContainer, FutureSalts
from pyrogram.utils import get_running_loop, _cancel, retry_range
from .internals import MsgId
from .internals.messagepacker import MessagePacker
from .internals.mtprotostate import MTProtoState
from .internals.requeststate import RequestState

log = logging.getLogger(__name__)


class Result:
    def __init__(self):
        self.value = None
        self.event = asyncio.Event()


class Session:
    START_TIMEOUT = 7
    WAIT_TIMEOUT = 15
    SLEEP_THRESHOLD = 10
    MAX_RETRIES = 10
    ACKS_THRESHOLD = 10
    PING_INTERVAL = 20
    STORED_MSG_IDS_MAX_SIZE = 1000 * 2

    TRANSPORT_ERRORS = {
        404: "auth key not found",
        429: "transport flood",
        444: "invalid DC"
    }

    def __init__(
            self,
            client: "pyrogram.Client",
            dc_id: int,
            auth_key: bytes,
            test_mode: bool,
            is_media: bool = False,
            is_cdn: bool = False
    ):
        self.client = client
        self.dc_id = dc_id
        self.auth_key = auth_key
        self.test_mode = test_mode
        self.is_media = is_media
        self.is_cdn = is_cdn

        self.connection = None
        self.connection_type = get_connection_class()
        self.init_proxy_params = None
        self.init_proxy()

        self.auth_key_id = sha1(auth_key).digest()[-8:]

        self.session_id = os.urandom(8)

        self.salt = 0

        self.stored_msg_ids = []

        self.loop = asyncio.get_event_loop()

        self._connect_lock = asyncio.Lock()

        # Whether the user has explicitly connected or disconnected.
        #
        # If a disconnection happens for any other reason and it
        # was *not* user action then the pending messages won't
        # be cleared but on explicit user disconnection all the
        # pending futures should be cancelled.
        self._user_connected = False
        self._reconnecting = False
        self._disconnected = self.loop.create_future()
        self._disconnected.set_result(None)

        # We need to join the loops upon disconnection
        self._send_loop_handle = None
        self._recv_loop_handle = None
        self._ping_loop_handle = None

        # Preserving the references of the AuthKey and state is important
        self._state = MTProtoState()

        # Outgoing messages are put in a queue and sent in a batch.
        # Note that here we're also storing their ``_RequestState``.
        self._send_queue = MessagePacker(self._state)

        # Sent states are remembered until a response is received.
        self._pending_state: Dict[int, RequestState] = {}

        # Responses must be acknowledged, and we can also batch these.
        self.pending_acks = set()

        # Similar to pending_messages but only for the last acknowledges.
        # These can't go in pending_messages because no acknowledge for them
        # is received, but we may still need to resend their state on bad salts.
        self._last_acks = collections.deque(maxlen=10)

    def init_proxy(self):
        if not self.client.proxy:
            return
        self.connection_type = get_connection_class(self.client.proxy.get("connection_type"))
        if issubclass(self.connection_type, TcpMTProxy):
            proxy_info = self.connection_type.address_info(self.client.proxy)
            self.init_proxy_params = raw.types.InputClientProxy(address=proxy_info[0], port=proxy_info[1])

    async def start(self):
        await self.connect(
            self.connection_type(
                self.dc_id,
                self.test_mode,
                self.client.ipv6,
                self.client.proxy,
                self.is_media,
            )
        )
        try:
            # await self.send(raw.functions.Ping(ping_id=0), timeout=self.START_TIMEOUT)

            if not self.is_cdn:
                await self.send(
                    raw.functions.InvokeWithLayer(
                        layer=layer,
                        query=raw.functions.InitConnection(
                            api_id=await self.client.storage.api_id(),
                            app_version=self.client.app_version,
                            device_model=self.client.device_model,
                            system_version=self.client.system_version,
                            system_lang_code=self.client.system_lang_code,
                            lang_pack=self.client.lang_pack,
                            lang_code=self.client.lang_code,
                            query=raw.functions.help.GetConfig(),
                            params=self.client.init_connection_params,
                            proxy=self.init_proxy_params,
                        )
                    ),
                    timeout=self.START_TIMEOUT,
                )

            log.info("Session initialized: Layer %s", layer)
            log.info("Device: %s - %s", self.client.device_model, self.client.app_version)
            log.info("System: %s (%s)", self.client.system_version, self.client.lang_code)
        except AuthKeyDuplicated as e:
            await self.stop()
            raise e
        except (OSError, RPCError):
            await self.stop()
        except Exception as e:
            await self.stop()
            raise e

        log.info("Session started")

    async def stop(self):
        self.stored_msg_ids.clear()

        await self.disconnect()

        if not self.is_media and callable(self.client.disconnect_handler):
            try:
                await self.client.disconnect_handler(self.client)
            except Exception as e:
                log.exception(e)

        log.info("Session stopped")

    async def restart(self):
        await self.stop()
        await self.start()

    async def connect(self, connection: "Connection"):
        """
        Connects to the specified given connection using the given auth key.
        """
        async with self._connect_lock:
            if self._user_connected:
                log.info('User is already connected!')
                return False

            self.connection = connection
            await self._connect()
            self._user_connected = True
            return True

    def is_connected(self):
        return self._user_connected

    def _transport_connected(self):
        return (
                not self._reconnecting
                and self.connection is not None
                and self.connection._connected
        )

    async def disconnect(self):
        """
        Cleanly disconnects the instance from the network, cancels
        all pending requests, and closes the send and receive loops.
        """
        await self._disconnect()

    @property
    def disconnected(self):
        """
        Future that resolves when the connection to Telegram
        ends, either by user action or in the background.

        Note that it may resolve in either a ``ConnectionError``
        or any other unexpected error that could not be handled.
        """
        return asyncio.shield(self._disconnected)

    def _start_reconnect(self, error):
        """Starts a reconnection in the background."""
        if self._user_connected and not self._reconnecting:
            # We set reconnecting to True here and not inside the new task
            # because it may happen that send/recv loop calls this again
            # while the new task hasn't had a chance to run yet. This race
            # condition puts `self.connection` in a bad state with two calls
            # to its `connect` without disconnecting, so it creates a second
            # receive loop. There can't be two tasks receiving data from
            # the reader, since that causes an error, and the library just
            # gets stuck.
            # TODO It still gets stuck? Investigate where and why.
            self._reconnecting = True
            get_running_loop().create_task(self._reconnect(error))

    async def _disconnect(self, error=None):
        if self.connection is None:
            log.info('Not disconnecting (already have no connection)')
            return

        log.info('Disconnecting from %s...', self.connection)
        self._user_connected = False
        try:
            log.debug('Closing current connection...')
            await self.connection.disconnect()
        finally:
            log.debug('Cancelling %d pending message(s)...', len(self._pending_state))
            for state in self._pending_state.values():
                if error and not state.future.done():
                    state.future.set_exception(error)
                else:
                    state.future.cancel()

            self._pending_state.clear()
            await _cancel(
                log,
                send_loop_handle=self._send_loop_handle,
                recv_loop_handle=self._recv_loop_handle,
                ping_loop_handle=self._ping_loop_handle,
            )

            log.info('Disconnection from %s complete!', self.connection)
            self.connection = None

        if self._disconnected and not self._disconnected.done():
            if error:
                self._disconnected.set_exception(error)
            else:
                self._disconnected.set_result(None)

    async def _reconnect(self, last_error):
        """
        Cleanly disconnects and then reconnects.
        """
        log.info('Closing current connection to begin reconnect...')
        await self.connection.disconnect()

        await _cancel(
            log,
            send_loop_handle=self._send_loop_handle,
            recv_loop_handle=self._recv_loop_handle,
            ping_loop_handle=self._ping_loop_handle,
        )

        # TODO See comment in `_start_reconnect`
        # Perhaps this should be the last thing to do?
        # But _connect() creates tasks which may run and,
        # if they see that reconnecting is True, they will end.
        # Perhaps that task creation should not belong in connect?
        self._reconnecting = False

        attempt = 0
        ok = True
        # We're already "retrying" to connect, so we don't want to force retries
        for attempt in retry_range(self.MAX_RETRIES, force_retry=False):
            try:
                await self._connect()
            except (IOError, asyncio.TimeoutError) as e:
                last_error = e
                log.info('Failed reconnection attempt %d with %s',
                         attempt, e.__class__.__name__)
                await asyncio.sleep(1)
            except BufferError as e:
                # TODO there should probably only be one place to except all these errors
                if isinstance(e, InvalidBufferError) and e.code == 404:
                    log.info('Server does not know about the current auth key; the session may need to be recreated')
                    last_error = AuthKeyDuplicated()
                    ok = False
                    break
                else:
                    log.warning('Invalid buffer %s', e)

            except Exception as e:
                last_error = e
                log.exception('Unexpected exception reconnecting on '
                              'attempt %d', attempt)

                await asyncio.sleep(1)
            else:
                self._send_queue.extend(self._pending_state.values())
                self._pending_state.clear()

                break
        else:
            ok = False

        if not ok:
            log.error('Automatic reconnection failed %d time(s)', attempt)
            # There may be no error (e.g. automatic reconnection was turned off).
            error = last_error.with_traceback(None) if last_error else None
            await self._disconnect(error=error)

    async def _connect(self):
        """
        Performs the actual connection, retrying, generating the
        authorization key if necessary, and starting the send and
        receive loops.
        """
        log.info('Connecting to %s...', self.connection)

        connected = False

        for attempt in retry_range(self.MAX_RETRIES):
            if not connected:
                connected = await self._try_connect(attempt)
                if not connected:
                    continue
            break  # all steps done, break retry loop
        else:
            if not connected:
                raise ConnectionError('Connection to Telegram failed {} time(s)'.format(self.MAX_RETRIES))

            e = ConnectionError('auth_key generation failed {} time(s)'.format(self.MAX_RETRIES))
            await self._disconnect(error=e)
            raise e

        loop = get_running_loop()
        log.debug('Starting send loop')
        self._send_loop_handle = loop.create_task(self._send_loop())

        log.debug('Starting receive loop')
        self._recv_loop_handle = loop.create_task(self._recv_loop())

        log.debug('Starting ping loop')
        self._ping_loop_handle = loop.create_task(self._ping_loop())

        # _disconnected only completes after manual disconnection
        # or errors after which the sender cannot continue such
        # as failing to reconnect or any unexpected error.
        if self._disconnected.done():
            self._disconnected = loop.create_future()

        log.info('Connection to %s complete!', self.connection)

    async def _try_connect(self, attempt):
        try:
            log.debug('Connection attempt %d...', attempt)
            await self.connection.connect()
            log.debug('Connection success!')
            return True
        except (IOError, asyncio.TimeoutError) as e:
            log.warning('Attempt %d at connecting failed: %s: %s',
                        attempt, type(e).__name__, e)
            await asyncio.sleep(1)
            return False

    # Loops

    async def _send_loop(self):
        """
        This loop is responsible for popping items off the send
        queue, encrypting them, and sending them over the network.

        Besides `connect`, only this method ever sends data.
        """
        while self._user_connected and not self._reconnecting:
            if self.pending_acks:
                ack = RequestState(raw.types.MsgsAck(msg_ids=list(self.pending_acks)))
                self._send_queue.append(ack)
                self._last_acks.append(ack)
                self.pending_acks.clear()

            log.debug('Waiting for messages to send...')
            # TODO Wait for the connection send queue to be empty?
            # This means that while it's not empty we can wait for
            # more messages to be added to the send queue.
            batch, data = await self._send_queue.get()

            if not data:
                continue

            log.debug('Encrypting %d message(s) in %d bytes for sending',
                      len(batch), len(data))

            data = await self.loop.run_in_executor(
                pyrogram.crypto_executor,
                mtproto.pack,
                data,
                self.salt,
                self.session_id,
                self.auth_key,
                self.auth_key_id
            )

            # Whether sending succeeds or not, the popped requests are now
            # pending because they're removed from the queue. If a reconnect
            # occurs, they will be removed from pending state and re-enqueued
            # so even if the network fails they won't be lost. If they were
            # never re-enqueued, the future waiting for a response "locks".
            for state in batch:
                if not isinstance(state, list):
                    if state.need_wait:
                        self._pending_state[state.msg_id] = state
                else:
                    for s in state:
                        if s.need_wait:
                            self._pending_state[s.msg_id] = s

            try:
                await self.connection.send(data)
            except IOError as e:
                log.info('Connection closed while sending data')
                self._start_reconnect(e)
                return

            log.debug('Encrypted messages put in a queue to be sent')

    async def _recv_loop(self):
        """
        This loop is responsible for reading all incoming responses
        from the network, decrypting and handling or dispatching them.

        Besides `connect`, only this method ever receives data.
        """
        while self._user_connected and not self._reconnecting:
            log.debug('Receiving items from the network...')
            try:
                body = await self.connection.recv()
            except asyncio.CancelledError:
                raise  # bypass except Exception
            except (IOError, asyncio.IncompleteReadError) as e:
                log.info('Connection closed while receiving data: %s', e)
                self._start_reconnect(e)
                return
            except InvalidBufferError as e:
                if e.code == 429:
                    log.warning('Server indicated flood error at transport level: %s', e)
                    await self._disconnect(error=e)
                else:
                    log.exception('Server sent invalid buffer')
                    self._start_reconnect(e)
                return
            except Exception as e:
                log.exception('Unhandled error while receiving data')
                self._start_reconnect(e)
                return

            try:
                message = await self.loop.run_in_executor(
                    pyrogram.crypto_executor,
                    mtproto.unpack,
                    BytesIO(body),
                    self.session_id,
                    self.auth_key,
                    self.auth_key_id
                )
                if message is None:
                    continue  # this message is to be ignored
            except SecurityError as e:
                # A step while decoding had the incorrect data. This message
                # should not be considered safe and it should be ignored.
                log.warning('Security error while unpacking a '
                            'received message: %s', e)
                continue
            except BufferError as e:
                if isinstance(e, InvalidBufferError) and e.code == 404:
                    log.info('Server does not know about the current auth key; the session may need to be recreated')
                    await self._disconnect(error=AuthKeyDuplicated())
                else:
                    log.warning('Invalid buffer %s', e)
                    self._start_reconnect(e)
                return
            except Exception as e:
                log.exception('Unhandled error while decrypting data')
                self._start_reconnect(e)
                return

            try:
                await self._process_message(message)
            except Exception:
                log.exception('Unhandled error while processing msgs')

    async def _process_message(self, data):
        messages = (
            data.body.messages
            if isinstance(data.body, MsgContainer)
            else [data]
        )

        log.debug("Received: %s", data)

        for msg in messages:
            if msg.seq_no % 2 != 0:
                if msg.msg_id in self.pending_acks:
                    continue
                else:
                    self.pending_acks.add(msg.msg_id)

            try:
                if len(self.stored_msg_ids) > Session.STORED_MSG_IDS_MAX_SIZE:
                    del self.stored_msg_ids[:Session.STORED_MSG_IDS_MAX_SIZE // 2]

                if self.stored_msg_ids:
                    if msg.msg_id < self.stored_msg_ids[0]:
                        raise SecurityCheckMismatch("The msg_id is lower than all the stored values")

                    if msg.msg_id in self.stored_msg_ids:
                        raise SecurityCheckMismatch("The msg_id is equal to any of the stored values")

                    time_diff = (msg.msg_id - MsgId()) / 2 ** 32

                    if time_diff > 30:
                        raise SecurityCheckMismatch("The msg_id belongs to over 30 seconds in the future. "
                                                    "Most likely the client time has to be synchronized.")

                    if time_diff < -300:
                        raise SecurityCheckMismatch("The msg_id belongs to over 300 seconds in the past. "
                                                    "Most likely the client time has to be synchronized.")
            except SecurityCheckMismatch as e:
                log.info("Discarding packet: %s", e)
                await self.connection.disconnect()
                return
            else:
                bisect.insort(self.stored_msg_ids, msg.msg_id)

            if isinstance(msg.body, (raw.types.MsgDetailedInfo, raw.types.MsgNewDetailedInfo)):
                self.pending_acks.add(msg.body.answer_msg_id)
                continue

            if isinstance(msg.body, raw.types.NewSessionCreated):
                continue

            msg_id = None

            if isinstance(msg.body, (raw.types.BadMsgNotification, raw.types.BadServerSalt)):
                msg_id = msg.body.bad_msg_id
            elif isinstance(msg.body, (FutureSalts, raw.types.RpcResult)):
                msg_id = msg.body.req_msg_id
            elif isinstance(msg.body, raw.types.Pong):
                msg_id = msg.body.msg_id
            else:
                if self.client is not None:
                    get_running_loop().create_task(self.client.handle_updates(msg.body))

            state = self._pending_state.pop(msg_id, None)
            if state and not state.future.cancelled():
                state.future.set_result(getattr(msg.body, "result", msg.body))

    async def _ping_loop(self):
        while self._user_connected and not self._reconnecting:
            try:
                await asyncio.wait_for(self.disconnected, self.PING_INTERVAL)
                continue  # We actually just want to act upon timeout
            except asyncio.TimeoutError:
                pass
            except asyncio.CancelledError:
                return
            except Exception:
                continue  # Any disconnected exception should be ignored

            if not self._transport_connected():
                continue

            try:
                await self.send(
                    raw.functions.PingDelayDisconnect(
                        ping_id=0, disconnect_delay=self.WAIT_TIMEOUT + 10
                    ), False
                )
            except (ConnectionError, asyncio.CancelledError):
                break

        log.info("PingTask stopped")

    def _pop_states(self, msg_id: int):
        """
        Pops the states known to match the given ID from pending messages.

        This method should be used when the response isn't specific.
        """
        state = self._pending_state.pop(msg_id, None)
        if state:
            return [state]

        to_pop = []
        for state in self._pending_state.values():
            if state.container_id == msg_id:
                to_pop.append(state.msg_id)

        if to_pop:
            return [self._pending_state.pop(x) for x in to_pop]

        for ack in self._last_acks:
            if ack.msg_id == msg_id:
                return [ack]

        return []

    async def send(self, data: TLObject, wait_response: bool = True, timeout: float = WAIT_TIMEOUT):
        if not self._user_connected:
            raise ConnectionError('Cannot send requests while disconnected')
        state = RequestState(data)
        self._send_queue.append(state)

        if wait_response:
            try:
                result = await asyncio.wait_for(state.future, timeout)
            except asyncio.TimeoutError:
                raise TimeoutError("Request timed out")

            if isinstance(result, raw.types.RpcError):
                if isinstance(data, (raw.functions.InvokeWithoutUpdates, raw.functions.InvokeWithTakeout)):
                    data = data.query

                RPCError.raise_it(result, type(data))

            if isinstance(result, raw.types.BadMsgNotification):
                log.warning("%s: %s", BadMsgNotification.__name__, BadMsgNotification(result.error_code))

            if isinstance(result, raw.types.BadServerSalt):
                self.salt = result.new_server_salt
                return await self.send(data, wait_response, timeout)

            return result

    async def invoke(
            self,
            query: TLObject,
            retries: int = MAX_RETRIES,
            timeout: float = WAIT_TIMEOUT,
            sleep_threshold: float = SLEEP_THRESHOLD
    ):
        if not self._user_connected:
            raise ConnectionError('Cannot send requests while disconnected')

        if isinstance(query, (raw.functions.InvokeWithoutUpdates, raw.functions.InvokeWithTakeout)):
            inner_query = query.query
        else:
            inner_query = query

        query_name = ".".join(inner_query.QUALNAME.split(".")[1:])

        while True:
            try:
                return await self.send(query, timeout=timeout)
            except FloodWait as e:
                amount = e.value

                if amount > sleep_threshold >= 0:
                    raise

                log.warning('[%s] Waiting for %s seconds before continuing (required by "%s")',
                            self.client.name, amount, query_name)

                await asyncio.sleep(amount)
            except (OSError, InternalServerError, ServiceUnavailable) as e:
                if retries == 0:
                    raise e from None

                (log.warning if retries < 2 else log.info)(
                    '[%s] Retrying "%s" due to: %s',
                    Session.MAX_RETRIES - retries + 1,
                    query_name, str(e) or repr(e)
                )

                await asyncio.sleep(0.5)

                return await self.invoke(query, retries - 1, timeout)
