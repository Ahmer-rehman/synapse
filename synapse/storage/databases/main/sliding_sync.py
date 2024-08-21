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


from typing import TYPE_CHECKING, Dict, List, Mapping, Optional, cast

import attr
from canonicaljson import encode_canonical_json

from synapse.api.errors import SlidingSyncUnknownPosition
from synapse.storage._base import SQLBaseStore, db_to_json
from synapse.storage.database import LoggingTransaction
from synapse.types import MultiWriterStreamToken, RoomStreamToken
from synapse.types.handlers.sliding_sync import (
    HaveSentRoom,
    HaveSentRoomFlag,
    MutablePerConnectionState,
    PerConnectionState,
    RoomStatusMap,
    RoomSyncConfig,
)

if TYPE_CHECKING:
    from synapse.storage.databases.main import DataStore


class SlidingSyncStore(SQLBaseStore):
    async def persist_per_connection_state(
        self,
        user_id: str,
        device_id: str,
        conn_id: str,
        previous_connection_position: Optional[int],
        per_connection_state: "MutablePerConnectionState",
    ) -> int:
        store = cast("DataStore", self)
        return await self.db_pool.runInteraction(
            "persist_per_connection_state",
            self.persist_per_connection_state_txn,
            user_id=user_id,
            device_id=device_id,
            conn_id=conn_id,
            previous_connection_position=previous_connection_position,
            per_connection_state=await PerConnectionStateDB.from_state(
                per_connection_state, store
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
            sql = """
                SELECT connection_key
                FROM sliding_sync_connection_state_positions
                INNER JOIN sliding_sync_connection_state USING (connection_key)
                WHERE
                    connection_position = ?
                    AND user_id = ? AND device_id = ? AND conn_id = ?
            """
            txn.execute(
                sql, (previous_connection_position, user_id, device_id, conn_id)
            )
            row = txn.fetchone()
            if row is None:
                raise SlidingSyncUnknownPosition()

            (connection_key,) = row
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
                txn,
                table="sliding_sync_connection_required_state",
                keyvalues={"connection_key": connection_key},
                retcols=("required_state_id", "required_state"),
            )
            for required_state_id, required_state in rows:
                required_state_to_id[required_state] = required_state_id

        room_to_state_ids: Dict[str, int] = {}
        unique_required_state: Dict[str, List[str]] = {}
        for room_id, room_state in per_connection_state.room_configs.items():
            serialized_state = encode_canonical_json(
                {
                    event_type: list(state_keys)
                    for event_type, state_keys in room_state.required_state_map.items()
                }
            ).decode()

            existing_state_id = required_state_to_id.get(serialized_state)
            if existing_state_id is not None:
                room_to_state_ids[room_id] = existing_state_id
            else:
                unique_required_state.setdefault(serialized_state, []).append(room_id)

        for serialized_required_state, room_ids in unique_required_state.items():
            (required_state_id,) = self.db_pool.simple_insert_returning_txn(
                txn,
                table="sliding_sync_connection_required_state",
                values={
                    "connection_key": connection_key,
                    "required_state": serialized_required_state,
                },
                returning=("required_state_id",),
            )
            for room_id in room_ids:
                room_to_state_ids[room_id] = required_state_id

        if previous_connection_position is not None:
            sql = """
                INSERT INTO sliding_sync_connection_streams
                (connection_position, stream, room_id, room_status, last_position)
                SELECT ?, stream, room_id, room_status, last_position
                FROM sliding_sync_connection_streams
                WHERE connection_position = ?
            """
            txn.execute(sql, (connection_position, previous_connection_position))

        key_values = []
        value_values = []
        for room_id, have_sent_room in per_connection_state.rooms._statuses.items():
            key_values.append((connection_position, "rooms", room_id))
            value_values.append(
                (have_sent_room.status.value, have_sent_room.last_token)
            )

        for room_id, have_sent_room in per_connection_state.receipts._statuses.items():
            key_values.append((connection_position, "receipts", room_id))
            value_values.append(
                (have_sent_room.status.value, have_sent_room.last_token)
            )

        self.db_pool.simple_upsert_many_txn(
            txn,
            table="sliding_sync_connection_streams",
            key_names=(
                "connection_position",
                "stream",
                "room_id",
            ),
            key_values=key_values,
            value_names=(
                "room_status",
                "last_position",
            ),
            value_values=value_values,
        )

        self.db_pool.simple_insert_many_txn(
            txn,
            table="sliding_sync_connection_room_configs",
            keys=(
                "connection_position",
                "room_id",
                "timeline_limit",
                "required_state_id",
            ),
            values=[
                (
                    connection_position,
                    room_id,
                    room_config.timeline_limit,
                    room_to_state_ids[room_id],
                )
                for room_id, room_config in per_connection_state.room_configs.items()
            ],
        )

        return connection_position

    @cached(iterable=True)
    async def get_per_connection_state(
        self, user_id: str, device_id: str, conn_id: str, connection_position: int
    ) -> "PerConnectionState":
        per_connection_state_db = await self.db_pool.runInteraction(
            "get_per_connection_state",
            self._get_per_connection_state_txn,
            user_id=user_id,
            device_id=device_id,
            conn_id=conn_id,
            connection_position=connection_position,
        )
        store = cast("DataStore", self)
        return await per_connection_state_db.to_state(store)

    def _get_per_connection_state_txn(
        self,
        txn: LoggingTransaction,
        user_id: str,
        device_id: str,
        conn_id: str,
        connection_position: int,
    ) -> "PerConnectionStateDB":
        sql = """
            SELECT connection_key
            FROM sliding_sync_connection_state_positions
            INNER JOIN sliding_sync_connection_state USING (connection_key)
            WHERE
                connection_position = ?
                AND user_id = ? AND device_id = ? AND conn_id = ?
        """
        txn.execute(sql, (connection_position, user_id, device_id, conn_id))
        row = txn.fetchone()
        if row is None:
            raise SlidingSyncUnknownPosition()

        (connection_key,) = row

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
            row[0]: {
                event_type: set(state_keys)
                for event_type, state_keys in db_to_json(row[1]).items()
            }
            for row in rows
        }

        room_config_rows = self.db_pool.simple_select_list_txn(
            txn,
            table="sliding_sync_connection_room_configs",
            keyvalues={"connection_position": connection_position},
            retcols=(
                "room_id",
                "timeline_limit",
                "required_state_id",
            ),
        )

        rooms: Dict[str, HaveSentRoom[str]] = {}
        receipts: Dict[str, HaveSentRoom[str]] = {}
        room_configs: Dict[str, RoomSyncConfig] = {}

        for (
            room_id,
            timeline_limit,
            required_state_id,
        ) in room_config_rows:
            room_configs[room_id] = RoomSyncConfig(
                timeline_limit=timeline_limit,
                required_state_map=required_state_map[required_state_id],
            )

        sql = """
            SELECT 'rooms', room_id, room_status, last_position FROM sliding_sync_connection_rooms
            UNION ALL
            SELECT 'receipts', room_id, room_status, last_position FROM sliding_sync_connection_receipts
        """

        # TODO receipts
        receipt_rows = self.db_pool.simple_select_list_txn(
            txn,
            table="sliding_sync_connection_streams",
            keyvalues={"connection_position": connection_position},
            retcols=(
                "stream",
                "room_id",
                "room_status",
                "last_position",
            ),
        )
        for stream, room_id, room_status, last_position in receipt_rows:
            have_sent_room: HaveSentRoom[str] = HaveSentRoom(
                status=HaveSentRoomFlag(room_status), last_token=last_position
            )
            if stream == "rooms":
                rooms[room_id] = have_sent_room
            elif stream == "receipts":
                receipts[room_id] = have_sent_room

        return PerConnectionStateDB(
            rooms=RoomStatusMap(rooms),
            receipts=RoomStatusMap(receipts),
            room_configs=room_configs,
        )


@attr.s(auto_attribs=True)
class PerConnectionStateDB:
    rooms: "RoomStatusMap[str]"
    receipts: "RoomStatusMap[str]"

    room_configs: Mapping[str, "RoomSyncConfig"]

    @staticmethod
    async def from_state(
        per_connection_state: "MutablePerConnectionState", store: "DataStore"
    ) -> "PerConnectionStateDB":
        rooms = {
            room_id: HaveSentRoom(
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
            room_id: HaveSentRoom(
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
            rooms=RoomStatusMap(rooms),
            receipts=RoomStatusMap(receipts),
            room_configs=per_connection_state.room_configs.maps[0],
        )

    async def to_state(self, store: "DataStore") -> "PerConnectionState":
        rooms = {
            room_id: HaveSentRoom(
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
            room_id: HaveSentRoom(
                status=status.status,
                last_token=(
                    await MultiWriterStreamToken.parse(store, status.last_token)
                    if status.last_token is not None
                    else None
                ),
            )
            for room_id, status in self.receipts._statuses.items()
        }

        return PerConnectionState(
            rooms=RoomStatusMap(rooms),
            receipts=RoomStatusMap(receipts),
            room_configs=self.room_configs,
        )
