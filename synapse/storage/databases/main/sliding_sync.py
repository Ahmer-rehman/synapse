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


from typing import TYPE_CHECKING, Dict, List, Optional

from canonicaljson import encode_canonical_json

from synapse.storage._base import SQLBaseStore
from synapse.storage.database import LoggingTransaction
from synapse.util.iterutils import batch_iter

if TYPE_CHECKING:
    from synapse.handlers.sliding_sync import MutablePerConnectionState


class SlidingSyncStore(SQLBaseStore):
    async def persist_per_connection_state(
        self,
        user_id: str,
        device_id: str,
        conn_id: str,
        previous_connection_position: Optional[int],
        per_connection_state: "MutablePerConnectionState",
    ) -> None:
        pass

    def persist_per_connection_state_txn(
        self,
        txn: LoggingTransaction,
        user_id: str,
        device_id: str,
        conn_id: str,
        previous_connection_position: Optional[int],
        per_connection_state: "MutablePerConnectionState",
    ) -> int:
        # TODO: Get connection key
        if previous_connection_position is not None:
            connection_key = self.db_pool.simple_select_one_onecol(
                table="sliding_sync_connection_state_positions",
                keyvalues={"connection_position": previous_connection_position},
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
                    have_sent_room.status.name,
                    have_sent_room.last_token,
                    per_connection_state.previous_room_configs[room_id].timeline_limit,
                    room_to_state_ids[room_id],
                )
                for room_id, have_sent_room in per_connection_state.rooms.get_updates().items()
            ],
        )

        # TODO: receipts
