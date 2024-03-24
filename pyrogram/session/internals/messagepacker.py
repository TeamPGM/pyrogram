import asyncio
import collections
import logging
from typing import Tuple, List, Union, Optional, Deque

from pyrogram.raw.core import MsgContainer, TLObject, Message
from pyrogram.session.internals.mtprotostate import MTProtoState
from pyrogram.session.internals.requeststate import RequestState


class MessagePacker:
    """
    This class packs `RequestState` as outgoing `TLMessages`.

    The purpose of this class is to support putting N `RequestState` into a
    queue, and then awaiting for "packed" `TLMessage` in the other end. The
    simplest case would be ``State -> TLMessage`` (1-to-1 relationship) but
    for efficiency purposes it's ``States -> Container`` (N-to-1).

    This addresses several needs: outgoing messages will be smaller, so the
    encryption and network overhead also is smaller. It's also a central
    point where outgoing requests are put, and where ready-messages are get.
    """

    def __init__(self, state: "MTProtoState"):
        self._state = state
        self._deque: Deque["RequestState"] = collections.deque()
        self._ready = asyncio.Event()
        self._log = logging.getLogger(__name__)

    def append(self, state: RequestState):
        self._deque.append(state)
        self._ready.set()

    def extend(self, states):
        self._deque.extend(states)
        self._ready.set()

    async def get(self) -> Union[Tuple[None, None], Tuple[List[RequestState], Optional[Message]]]:
        """
        Returns (batch, data) if one or more items could be retrieved.

        If the cancellation occurs or only invalid items were in the
        queue, (None, None) will be returned instead.
        """
        if not self._deque:
            self._ready.clear()
            await self._ready.wait()

        batch = []
        packed = None
        size = 0

        # Fill a new batch to return while the size is small enough,
        # as long as we don't exceed the maximum length of messages.
        while self._deque and len(batch) <= MsgContainer.MAXIMUM_LENGTH:
            state: "RequestState" = self._deque.popleft()
            size += len(state.request) + Message.SIZE_OVERHEAD

            if size <= MsgContainer.MAXIMUM_SIZE:
                packed = self._state.write_data_as_message(
                    state.request, isinstance(state.request, TLObject),
                    after_id=state.after.msg_id if state.after else None
                )
                state.msg_id = packed.msg_id
                state.packed = packed
                batch.append(state)
                self._log.debug('Assigned msg_id = %d to %s (%x)',
                                state.msg_id, state.request.__class__.__name__,
                                id(state.request))
                continue

            if batch:
                # Put the item back since it can't be sent in this batch
                self._deque.appendleft(state)
                break

            # If a single message exceeds the maximum size, then the
            # message payload cannot be sent. Telegram would forcibly
            # close the connection; message would never be confirmed.
            #
            # We don't put the item back because it can never be sent.
            # If we did, we would loop again and reach this same path.
            # Setting the exception twice results in `InvalidStateError`
            # and this method should never return with error, which we
            # really want to avoid.
            self._log.warning(
                'Message payload for %s is too long (%d) and cannot be sent',
                state.request.__class__.__name__, len(state.request)
            )
            state.future.set_exception(
                ValueError('Request payload is too big'))

            size = 0
            continue

        if not batch:
            return None, None

        if len(batch) > 1:
            # Inlined code to pack several messages into a container
            data = MsgContainer(messages=[i.packed for i in batch])
            packed = self._state.write_data_as_message(
                data, content_related=False
            )
            for s in batch:
                s.container_id = packed.msg_id

        return batch, packed
