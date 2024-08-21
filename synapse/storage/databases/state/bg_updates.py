#
# This file is licensed under the Affero General Public License (AGPL) version 3.
#
# Copyright 2014-2016 OpenMarket Ltd
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
# Originally licensed under the Apache License, Version 2.0:
# <http://www.apache.org/licenses/LICENSE-2.0>.
#
# [This file includes modifications made by New Vector Limited]
#
#

import logging
from typing import TYPE_CHECKING, Dict, List, Mapping, Optional, Tuple, Union

from typing_extensions import assert_never

from synapse.api.constants import Membership
from synapse.logging.opentracing import tag_args, trace
from synapse.storage._base import SQLBaseStore, db_to_json, make_in_list_sql_clause
from synapse.storage.database import (
    DatabasePool,
    LoggingDatabaseConnection,
    LoggingTransaction,
)
from synapse.storage.databases.main.events import (
    SLIDING_SYNC_RELEVANT_STATE_SET,
    PersistEventsStore,
    SlidingSyncMembershipSnapshotSharedInsertValues,
)
from synapse.storage.engines import BaseDatabaseEngine, PostgresEngine
from synapse.types import JsonDict, MutableStateMap, StateMap, StrCollection
from synapse.types.handlers import SLIDING_SYNC_DEFAULT_BUMP_EVENT_TYPES
from synapse.types.state import StateFilter
from synapse.util.caches import intern_string

if TYPE_CHECKING:
    from synapse.server import HomeServer

logger = logging.getLogger(__name__)


MAX_STATE_DELTA_HOPS = 100


class _BackgroundUpdates:
    SLIDING_SYNC_JOINED_ROOMS_BACKFILL = "sliding_sync_joined_rooms_backfill"
    SLIDING_SYNC_MEMBERSHIP_SNAPSHOTS_BACKFILL = (
        "sliding_sync_membership_snapshots_backfill"
    )


class StateGroupBackgroundUpdateStore(SQLBaseStore):
    """Defines functions related to state groups needed to run the state background
    updates.
    """

    @trace
    @tag_args
    def _count_state_group_hops_txn(
        self, txn: LoggingTransaction, state_group: int
    ) -> int:
        """Given a state group, count how many hops there are in the tree.

        This is used to ensure the delta chains don't get too long.
        """
        if isinstance(self.database_engine, PostgresEngine):
            sql = """
                WITH RECURSIVE state(state_group) AS (
                    VALUES(?::bigint)
                    UNION ALL
                    SELECT prev_state_group FROM state_group_edges e, state s
                    WHERE s.state_group = e.state_group
                )
                SELECT count(*) FROM state;
            """

            txn.execute(sql, (state_group,))
            row = txn.fetchone()
            if row and row[0]:
                return row[0]
            else:
                return 0
        else:
            # We don't use WITH RECURSIVE on sqlite3 as there are distributions
            # that ship with an sqlite3 version that doesn't support it (e.g. wheezy)
            next_group: Optional[int] = state_group
            count = 0

            while next_group:
                next_group = self.db_pool.simple_select_one_onecol_txn(
                    txn,
                    table="state_group_edges",
                    keyvalues={"state_group": next_group},
                    retcol="prev_state_group",
                    allow_none=True,
                )
                if next_group:
                    count += 1

            return count

    @trace
    @tag_args
    def _get_state_groups_from_groups_txn(
        self,
        txn: LoggingTransaction,
        groups: List[int],
        state_filter: Optional[StateFilter] = None,
    ) -> Mapping[int, StateMap[str]]:
        """
        Given a number of state groups, fetch the latest state for each group.

        Args:
            txn: The transaction object.
            groups: The given state groups that you want to fetch the latest state for.
            state_filter: The state filter to apply the state we fetch state from the database.

        Returns:
            Map from state_group to a StateMap at that point.
        """

        state_filter = state_filter or StateFilter.all()

        results: Dict[int, MutableStateMap[str]] = {group: {} for group in groups}

        if isinstance(self.database_engine, PostgresEngine):
            # Temporarily disable sequential scans in this transaction. This is
            # a temporary hack until we can add the right indices in
            txn.execute("SET LOCAL enable_seqscan=off")

            # The below query walks the state_group tree so that the "state"
            # table includes all state_groups in the tree. It then joins
            # against `state_groups_state` to fetch the latest state.
            # It assumes that previous state groups are always numerically
            # lesser.
            # This may return multiple rows per (type, state_key), but last_value
            # should be the same.
            sql = """
                WITH RECURSIVE sgs(state_group) AS (
                    VALUES(?::bigint)
                    UNION ALL
                    SELECT prev_state_group FROM state_group_edges e, sgs s
                    WHERE s.state_group = e.state_group
                )
                %s
            """

            overall_select_query_args: List[Union[int, str]] = []

            # This is an optimization to create a select clause per-condition. This
            # makes the query planner a lot smarter on what rows should pull out in the
            # first place and we end up with something that takes 10x less time to get a
            # result.
            use_condition_optimization = (
                not state_filter.include_others and not state_filter.is_full()
            )
            state_filter_condition_combos: List[Tuple[str, Optional[str]]] = []
            # We don't need to caclculate this list if we're not using the condition
            # optimization
            if use_condition_optimization:
                for etype, state_keys in state_filter.types.items():
                    if state_keys is None:
                        state_filter_condition_combos.append((etype, None))
                    else:
                        for state_key in state_keys:
                            state_filter_condition_combos.append((etype, state_key))
            # And here is the optimization itself. We don't want to do the optimization
            # if there are too many individual conditions. 10 is an arbitrary number
            # with no testing behind it but we do know that we specifically made this
            # optimization for when we grab the necessary state out for
            # `filter_events_for_client` which just uses 2 conditions
            # (`EventTypes.RoomHistoryVisibility` and `EventTypes.Member`).
            if use_condition_optimization and len(state_filter_condition_combos) < 10:
                select_clause_list: List[str] = []
                for etype, skey in state_filter_condition_combos:
                    if skey is None:
                        where_clause = "(type = ?)"
                        overall_select_query_args.extend([etype])
                    else:
                        where_clause = "(type = ? AND state_key = ?)"
                        overall_select_query_args.extend([etype, skey])

                    select_clause_list.append(
                        f"""
                        (
                            SELECT DISTINCT ON (type, state_key)
                                type, state_key, event_id
                            FROM state_groups_state
                            INNER JOIN sgs USING (state_group)
                            WHERE {where_clause}
                            ORDER BY type, state_key, state_group DESC
                        )
                        """
                    )

                overall_select_clause = " UNION ".join(select_clause_list)
            else:
                where_clause, where_args = state_filter.make_sql_filter_clause()
                # Unless the filter clause is empty, we're going to append it after an
                # existing where clause
                if where_clause:
                    where_clause = " AND (%s)" % (where_clause,)

                overall_select_query_args.extend(where_args)

                overall_select_clause = f"""
                    SELECT DISTINCT ON (type, state_key)
                        type, state_key, event_id
                    FROM state_groups_state
                    WHERE state_group IN (
                        SELECT state_group FROM sgs
                    ) {where_clause}
                    ORDER BY type, state_key, state_group DESC
                """

            for group in groups:
                args: List[Union[int, str]] = [group]
                args.extend(overall_select_query_args)

                txn.execute(sql % (overall_select_clause,), args)
                for row in txn:
                    typ, state_key, event_id = row
                    key = (intern_string(typ), intern_string(state_key))
                    results[group][key] = event_id
        else:
            max_entries_returned = state_filter.max_entries_returned()

            where_clause, where_args = state_filter.make_sql_filter_clause()
            # Unless the filter clause is empty, we're going to append it after an
            # existing where clause
            if where_clause:
                where_clause = " AND (%s)" % (where_clause,)

            # XXX: We could `WITH RECURSIVE` here since it's supported on SQLite 3.8.3
            # or higher and our minimum supported version is greater than that.
            #
            # We just haven't put in the time to refactor this.
            for group in groups:
                next_group: Optional[int] = group

                while next_group:
                    # We did this before by getting the list of group ids, and
                    # then passing that list to sqlite to get latest event for
                    # each (type, state_key). However, that was terribly slow
                    # without the right indices (which we can't add until
                    # after we finish deduping state, which requires this func)
                    args = [next_group]
                    args.extend(where_args)

                    txn.execute(
                        "SELECT type, state_key, event_id FROM state_groups_state"
                        " WHERE state_group = ? " + where_clause,
                        args,
                    )
                    results[group].update(
                        ((typ, state_key), event_id)
                        for typ, state_key, event_id in txn
                        if (typ, state_key) not in results[group]
                    )

                    # If the number of entries in the (type,state_key)->event_id dict
                    # matches the number of (type,state_keys) types we were searching
                    # for, then we must have found them all, so no need to go walk
                    # further down the tree... UNLESS our types filter contained
                    # wildcards (i.e. Nones) in which case we have to do an exhaustive
                    # search
                    if (
                        max_entries_returned is not None
                        and len(results[group]) == max_entries_returned
                    ):
                        break

                    next_group = self.db_pool.simple_select_one_onecol_txn(
                        txn,
                        table="state_group_edges",
                        keyvalues={"state_group": next_group},
                        retcol="prev_state_group",
                        allow_none=True,
                    )

        # The results shouldn't be considered mutable.
        return results


class StateBackgroundUpdateStore(StateGroupBackgroundUpdateStore):
    STATE_GROUP_DEDUPLICATION_UPDATE_NAME = "state_group_state_deduplication"
    STATE_GROUP_INDEX_UPDATE_NAME = "state_group_state_type_index"
    STATE_GROUPS_ROOM_INDEX_UPDATE_NAME = "state_groups_room_id_idx"
    STATE_GROUP_EDGES_UNIQUE_INDEX_UPDATE_NAME = "state_group_edges_unique_idx"

    CURRENT_STATE_EVENTS_STREAM_ORDERING_INDEX_UPDATE_NAME = (
        "current_state_events_stream_ordering_idx"
    )
    ROOM_MEMBERSHIPS_STREAM_ORDERING_INDEX_UPDATE_NAME = (
        "room_memberships_stream_ordering_idx"
    )
    LOCAL_CURRENT_MEMBERSHIP_STREAM_ORDERING_INDEX_UPDATE_NAME = (
        "local_current_membership_stream_ordering_idx"
    )

    def __init__(
        self,
        database: DatabasePool,
        db_conn: LoggingDatabaseConnection,
        hs: "HomeServer",
    ):
        super().__init__(database, db_conn, hs)
        self.db_pool.updates.register_background_update_handler(
            self.STATE_GROUP_DEDUPLICATION_UPDATE_NAME,
            self._background_deduplicate_state,
        )
        self.db_pool.updates.register_background_update_handler(
            self.STATE_GROUP_INDEX_UPDATE_NAME, self._background_index_state
        )
        self.db_pool.updates.register_background_index_update(
            self.STATE_GROUPS_ROOM_INDEX_UPDATE_NAME,
            index_name="state_groups_room_id_idx",
            table="state_groups",
            columns=["room_id"],
        )

        # `state_group_edges` can cause severe performance issues if duplicate
        # rows are introduced, which can accidentally be done by well-meaning
        # server admins when trying to restore a database dump, etc.
        # See https://github.com/matrix-org/synapse/issues/11779.
        # Introduce a unique index to guard against that.
        self.db_pool.updates.register_background_index_update(
            self.STATE_GROUP_EDGES_UNIQUE_INDEX_UPDATE_NAME,
            index_name="state_group_edges_unique_idx",
            table="state_group_edges",
            columns=["state_group", "prev_state_group"],
            unique=True,
            # The old index was on (state_group) and was not unique.
            replaces_index="state_group_edges_idx",
        )

        # These indices are needed to validate the foreign key constraint
        # when events are deleted.
        self.db_pool.updates.register_background_index_update(
            self.CURRENT_STATE_EVENTS_STREAM_ORDERING_INDEX_UPDATE_NAME,
            index_name="current_state_events_stream_ordering_idx",
            table="current_state_events",
            columns=["event_stream_ordering"],
        )
        self.db_pool.updates.register_background_index_update(
            self.ROOM_MEMBERSHIPS_STREAM_ORDERING_INDEX_UPDATE_NAME,
            index_name="room_memberships_stream_ordering_idx",
            table="room_memberships",
            columns=["event_stream_ordering"],
        )
        self.db_pool.updates.register_background_index_update(
            self.LOCAL_CURRENT_MEMBERSHIP_STREAM_ORDERING_INDEX_UPDATE_NAME,
            index_name="local_current_membership_stream_ordering_idx",
            table="local_current_membership",
            columns=["event_stream_ordering"],
        )

        # Backfill the sliding sync tables
        self.db_pool.updates.register_background_update_handler(
            _BackgroundUpdates.SLIDING_SYNC_JOINED_ROOMS_BACKFILL,
            self._sliding_sync_joined_rooms_backfill,
        )
        self.db_pool.updates.register_background_update_handler(
            _BackgroundUpdates.SLIDING_SYNC_MEMBERSHIP_SNAPSHOTS_BACKFILL,
            self._sliding_sync_membership_snapshots_backfill,
        )

    async def _background_deduplicate_state(
        self, progress: dict, batch_size: int
    ) -> int:
        """This background update will slowly deduplicate state by reencoding
        them as deltas.
        """
        last_state_group = progress.get("last_state_group", 0)
        rows_inserted = progress.get("rows_inserted", 0)
        max_group = progress.get("max_group", None)

        BATCH_SIZE_SCALE_FACTOR = 100

        batch_size = max(1, int(batch_size / BATCH_SIZE_SCALE_FACTOR))

        if max_group is None:
            rows = await self.db_pool.execute(
                "_background_deduplicate_state",
                "SELECT coalesce(max(id), 0) FROM state_groups",
            )
            max_group = rows[0][0]

        def reindex_txn(txn: LoggingTransaction) -> Tuple[bool, int]:
            new_last_state_group = last_state_group
            for count in range(batch_size):
                txn.execute(
                    "SELECT id, room_id FROM state_groups"
                    " WHERE ? < id AND id <= ?"
                    " ORDER BY id ASC"
                    " LIMIT 1",
                    (new_last_state_group, max_group),
                )
                row = txn.fetchone()
                if row:
                    state_group, room_id = row

                if not row or not state_group:
                    return True, count

                txn.execute(
                    "SELECT state_group FROM state_group_edges"
                    " WHERE state_group = ?",
                    (state_group,),
                )

                # If we reach a point where we've already started inserting
                # edges we should stop.
                if txn.fetchall():
                    return True, count

                txn.execute(
                    "SELECT coalesce(max(id), 0) FROM state_groups"
                    " WHERE id < ? AND room_id = ?",
                    (state_group, room_id),
                )
                # There will be a result due to the coalesce.
                (prev_group,) = txn.fetchone()  # type: ignore
                new_last_state_group = state_group

                if prev_group:
                    potential_hops = self._count_state_group_hops_txn(txn, prev_group)
                    if potential_hops >= MAX_STATE_DELTA_HOPS:
                        # We want to ensure chains are at most this long,#
                        # otherwise read performance degrades.
                        continue

                    prev_state_by_group = self._get_state_groups_from_groups_txn(
                        txn, [prev_group]
                    )
                    prev_state = prev_state_by_group[prev_group]

                    curr_state_by_group = self._get_state_groups_from_groups_txn(
                        txn, [state_group]
                    )
                    curr_state = curr_state_by_group[state_group]

                    if not set(prev_state.keys()) - set(curr_state.keys()):
                        # We can only do a delta if the current has a strict super set
                        # of keys

                        delta_state = {
                            key: value
                            for key, value in curr_state.items()
                            if prev_state.get(key, None) != value
                        }

                        self.db_pool.simple_delete_txn(
                            txn,
                            table="state_group_edges",
                            keyvalues={"state_group": state_group},
                        )

                        self.db_pool.simple_insert_txn(
                            txn,
                            table="state_group_edges",
                            values={
                                "state_group": state_group,
                                "prev_state_group": prev_group,
                            },
                        )

                        self.db_pool.simple_delete_txn(
                            txn,
                            table="state_groups_state",
                            keyvalues={"state_group": state_group},
                        )

                        self.db_pool.simple_insert_many_txn(
                            txn,
                            table="state_groups_state",
                            keys=(
                                "state_group",
                                "room_id",
                                "type",
                                "state_key",
                                "event_id",
                            ),
                            values=[
                                (state_group, room_id, key[0], key[1], state_id)
                                for key, state_id in delta_state.items()
                            ],
                        )

            progress = {
                "last_state_group": state_group,
                "rows_inserted": rows_inserted + batch_size,
                "max_group": max_group,
            }

            self.db_pool.updates._background_update_progress_txn(
                txn, self.STATE_GROUP_DEDUPLICATION_UPDATE_NAME, progress
            )

            return False, batch_size

        finished, result = await self.db_pool.runInteraction(
            self.STATE_GROUP_DEDUPLICATION_UPDATE_NAME, reindex_txn
        )

        if finished:
            await self.db_pool.updates._end_background_update(
                self.STATE_GROUP_DEDUPLICATION_UPDATE_NAME
            )

        return result * BATCH_SIZE_SCALE_FACTOR

    async def _background_index_state(self, progress: dict, batch_size: int) -> int:
        def reindex_txn(conn: LoggingDatabaseConnection) -> None:
            conn.rollback()
            if isinstance(self.database_engine, PostgresEngine):
                # postgres insists on autocommit for the index
                conn.engine.attempt_to_set_autocommit(conn.conn, True)
                try:
                    txn = conn.cursor()
                    txn.execute(
                        "CREATE INDEX CONCURRENTLY state_groups_state_type_idx"
                        " ON state_groups_state(state_group, type, state_key)"
                    )
                    txn.execute("DROP INDEX IF EXISTS state_groups_state_id")
                finally:
                    conn.engine.attempt_to_set_autocommit(conn.conn, False)
            else:
                txn = conn.cursor()
                txn.execute(
                    "CREATE INDEX state_groups_state_type_idx"
                    " ON state_groups_state(state_group, type, state_key)"
                )
                txn.execute("DROP INDEX IF EXISTS state_groups_state_id")

        await self.db_pool.runWithConnection(reindex_txn)

        await self.db_pool.updates._end_background_update(
            self.STATE_GROUP_INDEX_UPDATE_NAME
        )

        return 1

    async def _sliding_sync_joined_rooms_backfill(
        self, progress: JsonDict, batch_size: int
    ) -> int:
        """
        Handles backfilling the `sliding_sync_joined_rooms` table.
        """
        last_room_id = progress.get("last_room_id", "")

        def make_sql_clause_for_get_last_event_pos_in_room(
            database_engine: BaseDatabaseEngine,
            event_types: Optional[StrCollection] = None,
        ) -> Tuple[str, list]:
            """
            Returns the ID and event position of the last event in a room at or before a
            stream ordering.

            Based on `get_last_event_pos_in_room_before_stream_ordering(...)`

            Args:
                database_engine
                event_types: Optional allowlist of event types to filter by

            Returns:
                A tuple of SQL query and the args
            """
            event_type_clause = ""
            event_type_args: List[str] = []
            if event_types is not None and len(event_types) > 0:
                event_type_clause, event_type_args = make_in_list_sql_clause(
                    database_engine, "type", event_types
                )
                event_type_clause = f"AND {event_type_clause}"

            sql = f"""
            SELECT stream_ordering
            FROM events
            LEFT JOIN rejections USING (event_id)
            WHERE room_id = ?
                {event_type_clause}
                AND NOT outlier
                AND rejections.event_id IS NULL
            ORDER BY stream_ordering DESC
            LIMIT 1
            """

            return sql, event_type_args

        def _txn(txn: LoggingTransaction) -> int:
            # Fetch the set of room IDs that we want to update
            txn.execute(
                """
                SELECT DISTINCT room_id FROM current_state_events
                WHERE room_id > ?
                ORDER BY room_id ASC
                LIMIT ?
                """,
                (last_room_id, batch_size),
            )

            rooms_to_update_rows = txn.fetchall()
            if not rooms_to_update_rows:
                return 0

            for (room_id,) in rooms_to_update_rows:
                # TODO: Handle redactions
                current_state_map = PersistEventsStore._get_relevant_sliding_sync_current_state_event_ids_txn(
                    txn, room_id
                )
                # We're iterating over rooms pulled from the current_state_events table
                # so we should have some current state for each room
                assert current_state_map

                sliding_sync_joined_rooms_insert_map = PersistEventsStore._get_sliding_sync_insert_values_from_state_ids_map_txn(
                    txn, current_state_map
                )
                # We should have some insert values for each room, even if they are `None`
                assert sliding_sync_joined_rooms_insert_map

                (
                    most_recent_event_stream_ordering_clause,
                    most_recent_event_stream_ordering_args,
                ) = make_sql_clause_for_get_last_event_pos_in_room(
                    txn.database_engine, event_types=None
                )
                bump_stamp_clause, bump_stamp_args = (
                    make_sql_clause_for_get_last_event_pos_in_room(
                        txn.database_engine,
                        event_types=SLIDING_SYNC_DEFAULT_BUMP_EVENT_TYPES,
                    )
                )

                # Pulling keys/values separately is safe and will produce congruent
                # lists
                insert_keys = sliding_sync_joined_rooms_insert_map.keys()
                insert_values = sliding_sync_joined_rooms_insert_map.values()

                sql = f"""
                    INSERT INTO sliding_sync_joined_rooms
                        (room_id, event_stream_ordering, bump_stamp, {", ".join(insert_keys)})
                    VALUES (
                        ?,
                        ({most_recent_event_stream_ordering_clause}),
                        ({bump_stamp_clause}),
                        {", ".join("?" for _ in insert_values)}
                    )
                    ON CONFLICT (room_id)
                    DO UPDATE SET
                        event_stream_ordering = EXCLUDED.event_stream_ordering,
                        bump_stamp = EXCLUDED.bump_stamp,
                        {", ".join(f"{key} = EXCLUDED.{key}" for key in insert_keys)}
                    """
                args = (
                    [room_id, room_id]
                    + most_recent_event_stream_ordering_args
                    + [room_id]
                    + bump_stamp_args
                    + list(insert_values)
                )
                txn.execute(sql, args)

            self.db_pool.updates._background_update_progress_txn(
                txn,
                _BackgroundUpdates.SLIDING_SYNC_JOINED_ROOMS_BACKFILL,
                {"last_room_id": rooms_to_update_rows[-1][0]},
            )

            return len(rooms_to_update_rows)

        count = await self.db_pool.runInteraction(
            "sliding_sync_joined_rooms_backfill", _txn
        )

        if not count:
            await self.db_pool.updates._end_background_update(
                _BackgroundUpdates.SLIDING_SYNC_JOINED_ROOMS_BACKFILL
            )

        return count

    async def _sliding_sync_membership_snapshots_backfill(
        self, progress: JsonDict, batch_size: int
    ) -> int:
        """
        Handles backfilling the `sliding_sync_membership_snapshots` table.
        """
        last_event_stream_ordering = progress.get(
            "last_event_stream_ordering", -(1 << 31)
        )

        def _txn(txn: LoggingTransaction) -> int:
            # Fetch the set of event IDs that we want to update
            txn.execute(
                """
                SELECT
                    c.room_id,
                    c.user_id,
                    c.event_id,
                    c.membership,
                    c.event_stream_ordering,
                    e.outlier
                FROM local_current_membership as c
                INNER JOIN events AS e USING (event_id)
                WHERE event_stream_ordering > ?
                ORDER BY event_stream_ordering ASC
                LIMIT ?
                """,
                (last_event_stream_ordering, batch_size),
            )

            memberships_to_update_rows = txn.fetchall()
            if not memberships_to_update_rows:
                return 0

            for (
                room_id,
                user_id,
                membership_event_id,
                membership,
                membership_event_stream_ordering,
                is_outlier,
            ) in memberships_to_update_rows:
                # We don't know how to handle `membership` values other than these. The
                # code below would need to be updated.
                assert membership in (
                    Membership.JOIN,
                    Membership.INVITE,
                    Membership.KNOCK,
                    Membership.LEAVE,
                    Membership.BAN,
                )

                # Map of values to insert/update in the `sliding_sync_membership_snapshots` table
                sliding_sync_membership_snapshots_insert_map: (
                    SlidingSyncMembershipSnapshotSharedInsertValues
                ) = {}
                if membership == Membership.JOIN:
                    # If we're still joined, we can pull from current state
                    current_state_map = PersistEventsStore._get_relevant_sliding_sync_current_state_event_ids_txn(
                        txn, room_id
                    )
                    # We're iterating over rooms that we are joined to so they should
                    # have `current_state_events` and we should have some current state
                    # for each room
                    assert current_state_map

                    state_insert_values = PersistEventsStore._get_sliding_sync_insert_values_from_state_ids_map_txn(
                        txn, current_state_map
                    )
                    sliding_sync_membership_snapshots_insert_map.update(
                        state_insert_values
                    )
                    # We should have some insert values for each room, even if they are `None`
                    assert sliding_sync_membership_snapshots_insert_map

                    # We have current state to work from
                    sliding_sync_membership_snapshots_insert_map["has_known_state"] = (
                        True
                    )
                elif membership in (Membership.INVITE, Membership.KNOCK) or (
                    membership == Membership.LEAVE and is_outlier
                ):
                    invite_or_knock_event_id = membership_event_id
                    invite_or_knock_membership = membership

                    # If the event is an `out_of_band_membership` (special case of
                    # `outlier`), we never had historical state so we have to pull from
                    # the stripped state on the previous invite/knock event. This gives
                    # us a consistent view of the room state regardless of your
                    # membership (i.e. the room shouldn't disappear if your using the
                    # `is_encrypted` filter and you leave).
                    if membership == Membership.LEAVE and is_outlier:
                        # Find the previous invite/knock event before the leave event
                        txn.execute(
                            """
                            SELECT event_id, membership
                            FROM room_memberships
                            WHERE
                                room_id = ?
                                AND user_id = ?
                                AND event_stream_ordering < ?
                            ORDER BY event_stream_ordering DESC
                            LIMIT 1
                            """,
                            (
                                room_id,
                                user_id,
                                membership_event_stream_ordering,
                            ),
                        )
                        row = txn.fetchone()
                        # We should see a corresponding previous invite/knock event
                        assert row is not None
                        invite_or_knock_event_id, invite_or_knock_membership = row

                    # Pull from the stripped state on the invite/knock event
                    txn.execute(
                        """
                        SELECT json FROM event_json
                        WHERE event_id = ?
                        """,
                        (invite_or_knock_event_id,),
                    )
                    row = txn.fetchone()
                    # We should find a corresponding event
                    assert row is not None
                    json = row[0]
                    event_json = db_to_json(json)

                    raw_stripped_state_events = None
                    if invite_or_knock_membership == Membership.INVITE:
                        invite_room_state = event_json.get("unsigned").get(
                            "invite_room_state"
                        )
                        raw_stripped_state_events = invite_room_state
                    elif invite_or_knock_membership == Membership.KNOCK:
                        knock_room_state = event_json.get("unsigned").get(
                            "knock_room_state"
                        )
                        raw_stripped_state_events = knock_room_state

                    sliding_sync_membership_snapshots_insert_map = PersistEventsStore._get_sliding_sync_insert_values_from_stripped_state_txn(
                        txn, raw_stripped_state_events
                    )
                    # We should have some insert values for each room, even if no
                    # stripped state is on the event because we still want to record
                    # that we have no known state
                    assert sliding_sync_membership_snapshots_insert_map
                elif membership in (Membership.LEAVE, Membership.BAN):
                    # Pull from historical state
                    state_group = self.db_pool.simple_select_one_onecol_txn(
                        txn,
                        table="event_to_state_groups",
                        keyvalues={"event_id": membership_event_id},
                        retcol="state_group",
                        allow_none=True,
                    )
                    # We should know the state for the event
                    assert state_group is not None

                    state_by_group = self._get_state_groups_from_groups_txn(
                        txn,
                        groups=[state_group],
                        state_filter=StateFilter.from_types(
                            SLIDING_SYNC_RELEVANT_STATE_SET
                        ),
                    )
                    state_map = state_by_group[state_group]

                    state_insert_values = PersistEventsStore._get_sliding_sync_insert_values_from_state_ids_map_txn(
                        txn, state_map
                    )
                    sliding_sync_membership_snapshots_insert_map.update(
                        state_insert_values
                    )
                    # We should have some insert values for each room, even if they are `None`
                    assert sliding_sync_membership_snapshots_insert_map

                    # We have historical state to work from
                    sliding_sync_membership_snapshots_insert_map["has_known_state"] = (
                        True
                    )
                else:
                    assert_never(membership)

                # Pulling keys/values separately is safe and will produce congruent
                # lists
                insert_keys = sliding_sync_membership_snapshots_insert_map.keys()
                insert_values = sliding_sync_membership_snapshots_insert_map.values()
                # We don't need to do anything `ON CONFLICT` because we never partially
                # insert/update the snapshots
                txn.execute(
                    f"""
                    INSERT INTO sliding_sync_membership_snapshots
                        (room_id, user_id, membership_event_id, membership, event_stream_ordering
                        {("," + ", ".join(insert_keys)) if insert_keys else ""})
                    VALUES (
                        ?, ?, ?, ?,
                        (SELECT stream_ordering FROM events WHERE event_id = ?)
                        {("," + ", ".join("?" for _ in insert_values)) if insert_values else ""}
                    )
                    ON CONFLICT (room_id, user_id)
                    DO NOTHING
                    """,
                    [
                        room_id,
                        user_id,
                        membership_event_id,
                        membership,
                        membership_event_id,
                    ]
                    + list(insert_values),
                )

            (
                _room_id,
                _user_id,
                _membership_event_id,
                _membership,
                membership_event_stream_ordering,
                _is_outlier,
            ) = memberships_to_update_rows[-1]
            self.db_pool.updates._background_update_progress_txn(
                txn,
                _BackgroundUpdates.SLIDING_SYNC_MEMBERSHIP_SNAPSHOTS_BACKFILL,
                {"last_event_stream_ordering": membership_event_stream_ordering},
            )

            return len(memberships_to_update_rows)

        count = await self.db_pool.runInteraction(
            "sliding_sync_membership_snapshots_backfill", _txn
        )

        if not count:
            await self.db_pool.updates._end_background_update(
                _BackgroundUpdates.SLIDING_SYNC_MEMBERSHIP_SNAPSHOTS_BACKFILL
            )

        return count

    # async def _sliding_sync_membership_snapshots_backfill(
    #     self, progress: JsonDict, batch_size: int
    # ) -> int:
    #     """
    #     Handles backfilling the `sliding_sync_membership_snapshots` table.
    #     """
    #     last_event_stream_ordering = progress.get(
    #         "last_event_stream_ordering", -(1 << 31)
    #     )

    #     def _find_memberships_to_update_txn(
    #         txn: LoggingTransaction,
    #     ) -> List[Tuple[str, str, str, str, str, int, bool]]:
    #         # Fetch the set of event IDs that we want to update
    #         txn.execute(
    #             """
    #             SELECT
    #                 c.room_id,
    #                 c.user_id,
    #                 e.sender
    #                 c.event_id,
    #                 c.membership,
    #                 c.event_stream_ordering,
    #                 e.outlier
    #             FROM local_current_membership as c
    #             INNER JOIN events AS e USING (event_id)
    #             WHERE event_stream_ordering > ?
    #             ORDER BY event_stream_ordering ASC
    #             LIMIT ?
    #             """,
    #             (last_event_stream_ordering, batch_size),
    #         )

    #         memberships_to_update_rows = cast(
    #             List[Tuple[str, str, str, str, str, int, bool]], txn.fetchall()
    #         )

    #         return memberships_to_update_rows

    #     memberships_to_update_rows = await self.db_pool.runInteraction(
    #         "sliding_sync_membership_snapshots_backfill._find_memberships_to_update_txn",
    #         _find_memberships_to_update_txn,
    #     )

    #     if not memberships_to_update_rows:
    #         await self.db_pool.updates._end_background_update(
    #             _BackgroundUpdates.SLIDING_SYNC_MEMBERSHIP_SNAPSHOTS_BACKFILL
    #         )

    #     store = self.hs.get_storage_controllers().main

    #     def _find_previous_membership_txn(
    #         txn: LoggingTransaction, room_id: str, user_id: str, stream_ordering: int
    #     ) -> Tuple[str, str]:
    #         # Find the previous invite/knock event before the leave event
    #         txn.execute(
    #             """
    #             SELECT event_id, membership
    #             FROM room_memberships
    #             WHERE
    #                 room_id = ?
    #                 AND user_id = ?
    #                 AND event_stream_ordering < ?
    #             ORDER BY event_stream_ordering DESC
    #             LIMIT 1
    #             """,
    #             (
    #                 room_id,
    #                 user_id,
    #                 stream_ordering,
    #             ),
    #         )
    #         row = txn.fetchone()

    #         # We should see a corresponding previous invite/knock event
    #         assert row is not None
    #         event_id, membership = row

    #         return event_id, membership

    #     # Map from (room_id, user_id) to ...
    #     to_insert_membership_snapshots: Dict[
    #         Tuple[str, str], SlidingSyncMembershipSnapshotSharedInsertValues
    #     ] = {}
    #     to_insert_membership_infos: Dict[Tuple[str, str], SlidingSyncMembershipInfo] = (
    #         {}
    #     )
    #     for (
    #         room_id,
    #         user_id,
    #         sender,
    #         membership_event_id,
    #         membership,
    #         membership_event_stream_ordering,
    #         is_outlier,
    #     ) in memberships_to_update_rows:
    #         # We don't know how to handle `membership` values other than these. The
    #         # code below would need to be updated.
    #         assert membership in (
    #             Membership.JOIN,
    #             Membership.INVITE,
    #             Membership.KNOCK,
    #             Membership.LEAVE,
    #             Membership.BAN,
    #         )

    #         # Map of values to insert/update in the `sliding_sync_membership_snapshots` table
    #         sliding_sync_membership_snapshots_insert_map: (
    #             SlidingSyncMembershipSnapshotSharedInsertValues
    #         ) = {}
    #         if membership == Membership.JOIN:
    #             # If we're still joined, we can pull from current state.
    #             current_state_ids_map: StateMap[str] = (
    #                 await store.get_partial_filtered_current_state_ids(
    #                     room_id,
    #                     state_filter=StateFilter.from_types(
    #                         SLIDING_SYNC_RELEVANT_STATE_SET
    #                     ),
    #                 )
    #             )
    #             # We're iterating over rooms that we are joined to so they should
    #             # have `current_state_events` and we should have some current state
    #             # for each room
    #             assert current_state_ids_map

    #             fetched_events = await store.get_events(current_state_ids_map.values())

    #             current_state_map: StateMap[EventBase] = {
    #                 state_key: fetched_events[event_id]
    #                 for state_key, event_id in current_state_ids_map.items()
    #             }

    #             state_insert_values = EventsPersistenceStorageController._get_sliding_sync_insert_values_from_state_map(
    #                 current_state_map
    #             )
    #             sliding_sync_membership_snapshots_insert_map.update(state_insert_values)
    #             # We should have some insert values for each room, even if they are `None`
    #             assert sliding_sync_membership_snapshots_insert_map

    #             # We have current state to work from
    #             sliding_sync_membership_snapshots_insert_map["has_known_state"] = True
    #         elif membership in (Membership.INVITE, Membership.KNOCK) or (
    #             membership == Membership.LEAVE and is_outlier
    #         ):
    #             invite_or_knock_event_id = membership_event_id
    #             invite_or_knock_membership = membership

    #             # If the event is an `out_of_band_membership` (special case of
    #             # `outlier`), we never had historical state so we have to pull from
    #             # the stripped state on the previous invite/knock event. This gives
    #             # us a consistent view of the room state regardless of your
    #             # membership (i.e. the room shouldn't disappear if your using the
    #             # `is_encrypted` filter and you leave).
    #             if membership == Membership.LEAVE and is_outlier:
    #                 invite_or_knock_event_id, invite_or_knock_membership = (
    #                     await self.db_pool.runInteraction(
    #                         "sliding_sync_membership_snapshots_backfill._find_previous_membership",
    #                         _find_previous_membership_txn,
    #                         room_id,
    #                         user_id,
    #                         membership_event_stream_ordering,
    #                     )
    #                 )

    #             # Pull from the stripped state on the invite/knock event
    #             invite_or_knock_event = await store.get_event(invite_or_knock_event_id)

    #             raw_stripped_state_events = None
    #             if invite_or_knock_membership == Membership.INVITE:
    #                 invite_room_state = invite_or_knock_event.unsigned.get(
    #                     "invite_room_state"
    #                 )
    #                 raw_stripped_state_events = invite_room_state
    #             elif invite_or_knock_membership == Membership.KNOCK:
    #                 knock_room_state = invite_or_knock_event.unsigned.get(
    #                     "knock_room_state"
    #                 )
    #                 raw_stripped_state_events = knock_room_state

    #             sliding_sync_membership_snapshots_insert_map = await self.db_pool.runInteraction(
    #                 "sliding_sync_membership_snapshots_backfill._get_sliding_sync_insert_values_from_stripped_state_txn",
    #                 PersistEventsStore._get_sliding_sync_insert_values_from_stripped_state_txn,
    #                 raw_stripped_state_events,
    #             )

    #             # We should have some insert values for each room, even if no
    #             # stripped state is on the event because we still want to record
    #             # that we have no known state
    #             assert sliding_sync_membership_snapshots_insert_map
    #         elif membership in (Membership.LEAVE, Membership.BAN):
    #             # Pull from historical state
    #             state_group = await store._get_state_group_for_event(
    #                 membership_event_id
    #             )
    #             # We should know the state for the event
    #             assert state_group is not None

    #             state_by_group = await self.db_pool.runInteraction(
    #                 "sliding_sync_membership_snapshots_backfill._get_state_groups_from_groups_txn",
    #                 self._get_state_groups_from_groups_txn,
    #                 groups=[state_group],
    #                 state_filter=StateFilter.from_types(
    #                     SLIDING_SYNC_RELEVANT_STATE_SET
    #                 ),
    #             )
    #             state_ids_map = state_by_group[state_group]

    #             fetched_events = await store.get_events(state_ids_map.values())

    #             state_map: StateMap[EventBase] = {
    #                 state_key: fetched_events[event_id]
    #                 for state_key, event_id in state_ids_map.items()
    #             }

    #             state_insert_values = EventsPersistenceStorageController._get_sliding_sync_insert_values_from_state_map(
    #                 state_map
    #             )
    #             sliding_sync_membership_snapshots_insert_map.update(state_insert_values)
    #             # We should have some insert values for each room, even if they are `None`
    #             assert sliding_sync_membership_snapshots_insert_map

    #             # We have historical state to work from
    #             sliding_sync_membership_snapshots_insert_map["has_known_state"] = True
    #         else:
    #             assert_never(membership)

    #         to_insert_membership_snapshots[(room_id, user_id)] = (
    #             sliding_sync_membership_snapshots_insert_map
    #         )
    #         to_insert_membership_infos[(room_id, user_id)] = SlidingSyncMembershipInfo(
    #             user_id=user_id,
    #             sender=sender,
    #             membership_event_id=membership_event_id,
    #         )

    #     def _backfill_table_txn(txn: LoggingTransaction) -> None:
    #         for key, insert_map in to_insert_membership_snapshots.items():
    #             room_id, user_id = key
    #             membership_info = to_insert_membership_infos[key]
    #             membership_event_id = membership_info.membership_event_id
    #             membership = membership_info.membership
    #             membership_event_stream_ordering = (
    #                 membership_info.membership_event_stream_ordering
    #             )

    #             # Pulling keys/values separately is safe and will produce congruent
    #             # lists
    #             insert_keys = insert_map.keys()
    #             insert_values = insert_map.values()
    #             # We don't need to do anything `ON CONFLICT` because we never partially
    #             # insert/update the snapshots
    #             txn.execute(
    #                 f"""
    #                 INSERT INTO sliding_sync_membership_snapshots
    #                     (room_id, user_id, membership_event_id, membership, event_stream_ordering
    #                     {("," + ", ".join(insert_keys)) if insert_keys else ""})
    #                 VALUES (
    #                     ?, ?, ?,
    #                     (SELECT membership FROM room_memberships WHERE event_id = ?),
    #                     (SELECT stream_ordering FROM events WHERE event_id = ?)
    #                     {("," + ", ".join("?" for _ in insert_values)) if insert_values else ""}
    #                 )
    #                 ON CONFLICT (room_id, user_id)
    #                 DO NOTHING
    #                 """,
    #                 [
    #                     room_id,
    #                     user_id,
    #                     membership_event_id,
    #                     membership_event_id,
    #                     membership_event_id,
    #                 ]
    #                 + list(insert_values),
    #             )

    #     await self.db_pool.runInteraction(
    #         "sliding_sync_membership_snapshots_backfill", _backfill_table_txn
    #     )

    #     # Update the progress
    #     (
    #         _room_id,
    #         _user_id,
    #         _sender,
    #         _membership_event_id,
    #         _membership,
    #         membership_event_stream_ordering,
    #         _is_outlier,
    #     ) = memberships_to_update_rows[-1]
    #     await self.db_pool.updates._background_update_progress(
    #         _BackgroundUpdates.SLIDING_SYNC_MEMBERSHIP_SNAPSHOTS_BACKFILL,
    #         {"last_event_stream_ordering": membership_event_stream_ordering},
    #     )

    #     return len(memberships_to_update_rows)
