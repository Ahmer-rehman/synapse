#
# This file is licensed under the Affero General Public License (AGPL) version 3.
#
# Copyright (C) 2023 New Vector, Ltd
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# See the GNU Affero General Public License for more details:
# <https://www.gnu.org/licenses/agpl-3.0.html>.
#


from typing import TYPE_CHECKING, Dict, List, Mapping, Optional

import attr
from canonicaljson import encode_canonical_json

from synapse.handlers.sliding_sync import HaveSentRoom, HaveSentRoomFlag, RoomSyncConfig
from synapse.storage._base import SQLBaseStore, db_to_json
from synapse.storage.database import LoggingTransaction
from synapse.types import MultiWriterStreamToken, RoomStreamToken

if TYPE_CHECKING:
    from synapse.handlers.sliding_sync import (
        MutablePerConnectionState,
        PerConnectionState,
        RoomStatusesForStream,
    )


class SlidingSyncStore(SQLBaseStore):
    async def persist_per_connection_state(
        self,
        user_id: str,
        device_id: str,
        conn_id: str,
        previous_connection_position: Optional[int],
        per_connection_state: "MutablePerConnectionState",
    ) -> int:
        return await self.db_pool.runInteraction(
            "persist_per_connection_state",
            user_id=user_id,
            device_id=device_id,
            conn_id=conn_id,
            previous_connection_position=previous_connection_position,
            per_connection_state=await PerConnectionStateDB.from_state(
                per_connection_state
            ),
        )

    def persist_per_connection_state_txn(
        self,
        txn: LoggingTransaction,
        user_id: str,
        device_id: str,
        conn_id: str,
        previous_connection_position: Optional[int],
        per_connection_state: "PerConnectionStateDB",
    ) -> int:
        if previous_connection_position is not None:
            connection_key = self.db_pool.simple_select_one_onecol_txn(
                txn,
                table="sliding_sync_connection_state",
                keyvalues={
                    "connection_position": previous_connection_position,
                    "user_id": user_id,
                    "device_id": device_id,
                    "conn_id": conn_id,
                },
                retcol="connection_key",
                allow_none=True,
            )
            if connection_key is None:
                # TODO: Raise the unknown pos key
                raise Exception()
        else:
            (connection_key,) = self.db_pool.simple_insert_returning_txn(
                txn,
                table="sliding_sync_connection_state",
                values={
                    "user_id": user_id,
                    "device_id": device_id,
                    "conn_id": conn_id,
                    "created_ts": self._clock.time_msec(),
                },
                returning=("connection_key",),
            )

        (connection_position,) = self.db_pool.simple_insert_returning_txn(
            txn,
            table="sliding_sync_connection_state_positions",
            values={
                "connection_key": connection_key,
                "created_ts": self._clock.time_msec(),
            },
            returning=("connection_position",),
        )

        required_state_to_id: Dict[str, int] = {}

        if previous_connection_position is not None:
            rows = self.db_pool.simple_select_list_txn(
                table="sliding_sync_connection_required_state",
                keyvalues={"connection_key": connection_key},
                retcols=("required_state_id", "required_state"),
            )
            for required_state_id, required_state in rows:
                required_state_to_id[required_state] = required_state_id

        room_to_state_ids: Dict[str, int] = {}
        unique_required_state: Dict[str, List[str]] = {}
        for room_id, room_state in per_connection_state.previous_room_configs.items():
            serialized_state = encode_canonical_json(room_state.required_state_map)

            existing_state_id = required_state_to_id.get(serialized_state)
            if existing_state_id is not None:
                room_to_state_ids[room_id] = existing_state_id
            else:
                unique_required_state.setdefault(
                    serialized_state.decode("utf-8"), []
                ).append(room_id)

        for serialized_required_state, room_ids in unique_required_state.items():
            (required_state_id,) = self.db_pool.simple_insert_returning_txn(
                txn,
                table="sliding_sync_connection_required_state",
                values={
                    "connection_position": connection_position,
                    "required_state": serialized_required_state,
                },
                returning=("required_state_id",),
            )
            for room_id in room_ids:
                room_to_state_ids[room_id] = required_state_id

        if previous_connection_position is not None:
            sql = """
                INSERT INTO sliding_sync_connection_rooms
                (connection_position, room_id, room_status, last_position, timeline_limit, required_state_id)
                SELECT ?, room_id, room_status, last_position, timeline_limit, required_state_id
                FROM sliding_sync_connection_rooms
                WHERE connection_position = ?
            """
            txn.execute(sql, (connection_position, previous_connection_position))

        self.db_pool.simple_insert_many_txn(
            txn,
            table="sliding_sync_connection_rooms",
            keys=(
                "connection_position",
                "room_id",
                "room_status",
                "last_position",
                "timeline_limit",
                "required_state_id",
            ),
            values=[
                (
                    connection_position,
                    room_id,
                    have_sent_room.status.value,
                    have_sent_room.last_token,  # TODO: serialize...
                    per_connection_state.previous_room_configs[room_id].timeline_limit,
                    room_to_state_ids[room_id],
                )
                for room_id, have_sent_room in per_connection_state.rooms._statuses.items()
            ],
        )

        if previous_connection_position is not None:
            sql = """
                INSERT INTO sliding_sync_connection_receipts
                (connection_position, room_id, room_status, last_position)
                SELECT ?, room_id, room_status, last_position
                FROM sliding_sync_connection_receipts
                WHERE connection_position = ?
            """
            txn.execute(sql, (connection_position, previous_connection_position))

        self.db_pool.simple_insert_many_txn(
            txn,
            table="sliding_sync_connection_receipts",
            keys=(
                "connection_position",
                "room_id",
                "room_status",
                "last_position",
            ),
            values=[
                (
                    connection_position,
                    room_id,
                    have_sent_room.status.value,
                    have_sent_room.last_token,  # TODO: serialize...
                )
                for room_id, have_sent_room in per_connection_state.receipts._statuses.items()
            ],
        )

        return connection_position

    async def get_per_connection_state(
        self, user_id: str, device_id: str, conn_id: str, connection_position: int
    ) -> "PerConnectionState":
        return await self.db_pool.runInteraction(
            "get_per_connection_state",
            connection_position=connection_position,
        )

    def _get_per_connection_state_txn(
        self,
        txn: LoggingTransaction,
        user_id: str,
        device_id: str,
        conn_id: str,
        connection_position: int,
    ) -> "PerConnectionStateDB":
        connection_key = self.db_pool.simple_select_one_onecol_txn(
            txn,
            table="sliding_sync_connection_state",
            keyvalues={
                "connection_position": connection_position,
                "user_id": user_id,
                "device_id": device_id,
                "conn_id": conn_id,
            },
            retcol="connection_key",
            allow_none=True,
        )
        if connection_key is None:
            # TODO: Raise the unknown pos key
            raise Exception()

        sql = """
            DELETE FROM sliding_sync_connection_state_positions
            WHERE connection_key = ? AND connection_position != ?
        """
        txn.execute(sql, (connection_key, connection_position))

        rows = self.db_pool.simple_select_list_txn(
            txn,
            table="sliding_sync_connection_required_state",
            keyvalues={"connection_key": connection_key},
            retcols=(
                "required_state_id",
                "required_state",
            ),
        )

        required_state_map = {
            row["required_state_id"]: db_to_json(row["required_state"]) for row in rows
        }

        rooms = self.db_pool.simple_select_list_txn(
            txn,
            table="sliding_sync_connection_rooms",
            keyvalues={"connection_position": connection_position},
            retcols=(
                "room_id",
                "room_status",
                "last_position",
                "timeline_limit",
                "required_state_id",
            ),
        )

        rooms = {}
        previous_room_configs = {}

        for (
            room_id,
            room_status,
            last_position,
            timeline_limit,
            required_state_id,
        ) in rooms:
            rooms[room_id] = HaveSentRoom(
                status=HaveSentRoomFlag(room_status), last_position=last_position
            )


@attr.s(auto_attribs=True)
class PerConnectionStateDB:
    rooms: RoomStatusesForStream[str]
    receipts: RoomStatusesForStream[str]

    previous_room_configs: Mapping[str, RoomSyncConfig]

    @staticmethod
    async def from_state(
        per_connection_state: "MutablePerConnectionState", store: "SlidingSyncStore"
    ) -> "PerConnectionStateDB":
        rooms = {
            room_id: HaveSentRoomFlag(
                status=status.status,
                last_token=(
                    await status.last_token.to_string(store)
                    if status.last_token is not None
                    else None
                ),
            )
            for room_id, status in per_connection_state.rooms.get_updates().items()
        }

        receipts = {
            room_id: HaveSentRoomFlag(
                status=status.status,
                last_token=(
                    await status.last_token.to_string(store)
                    if status.last_token is not None
                    else None
                ),
            )
            for room_id, status in per_connection_state.receipts.get_updates().items()
        }

        return PerConnectionStateDB(
            rooms=rooms,
            receipts=receipts,
            previous_room_configs=per_connection_state.previous_room_configs,
        )

    async def to_state(self, store: "SlidingSyncStore") -> "PerConnectionState":
        rooms = {
            room_id: HaveSentRoomFlag(
                status=status.status,
                last_token=(
                    await RoomStreamToken.parse(store, status.last_token)
                    if status.last_token is not None
                    else None
                ),
            )
            for room_id, status in self.rooms._statuses.items()
        }

        receipts = {
            room_id: HaveSentRoomFlag(
                status=status.status,
                last_token=(
                    await MultiWriterStreamToken.parse(store, status.last_token)
                    if status.last_token is not None
                    else None
                ),
            )
            for room_id, status in self.receipts._statuses.items()
        }

        return PerConnectionStateDB(
            rooms=rooms,
            receipts=receipts,
            previous_room_configs=self.previous_room_configs,
        )
