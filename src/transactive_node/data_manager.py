"""
VolttronDataManager: the DataManager a TransactiveNodeAgent uses to archive through a VOLTTRON historian.

TENT itself knows nothing about VOLTTRON. Its components hand archive records to whatever DataManager the node was
configured with; this one forwards them over the message bus to the transactive-node historian
(volttron-transactive-node-historian), which owns the database. Two interactions:

- At start-up (``init_archive``) the table definitions every configured component declared are sent to the historian
  with the ``create_tables`` RPC, and the reply is waited for. Records queued before that moment are released once the
  historian has acknowledged the tables. The historian usually runs on another platform of the federation
  (``external_platform``), and a federation connection takes some time to come up after the platforms start, so an
  unanswered call is retried every ``rpc_retry_period`` seconds, each failure logged, until ``rpc_total_timeout``
  seconds have passed; then the error is raised rather than hidden: nothing would be archived in that run and the
  operator must know. An error reply (a rejected table definition) is raised at once; retrying cannot change it.
- Every batch of records (``archive_data``) is published as a list of ``{"table_name": ..., "data": {...}}`` envelopes
  on the agent's archive topic (``<db_topic>/<node name>/archive``), which the historian captures. Values are the
  JSON forms ``tent.utils.helpers.json_encoder`` produces (UUIDs and enumeration names as strings, datetimes as ISO
  8601, durations as seconds); the historian's LocalDataManager parses them back by column type.

Configuration (the ``data_manager`` block of the node config)::

    {"class_name": "VolttronDataManager", "module_name": "transactive_node.data_manager",
     "historian_vip": "tns.historian", "external_platform": "central", "rpc_timeout": 30}

``historian_vip`` must be the VIP identity the historian is installed under; it has no default because the two sides
have disagreed before and a silent mismatch simply archives nothing. ``external_platform`` is the instance name of the
platform the historian runs on, empty (the default) when it is the node's own platform.
"""
import logging
import time
from collections.abc import Mapping, Sequence

import gevent

from tent.data_manager import DataManager

_log = logging.getLogger(__name__)


class VolttronDataManager(DataManager):
    ARCHIVE_KEY = 'archive'  # publish key the agent maps to its archive topic

import logging
import time

from collections.abc import Mapping, Sequence
from uuid import uuid4

import gevent
from gevent.event import AsyncResult

from tent.data_manager import ARCHIVE_SCHEMA_VERSION, DataManager

_log = logging.getLogger(__name__)


class VolttronDataManager(DataManager):
    """Archives through the transactive-node historian on the message bus.

    Before anything is archived the node's table definitions are registered with the historian, together with this
    TENT's archive schema version (tent.data_manager.ARCHIVE_SCHEMA_VERSION), by one of two paths selected with
    `registration`:

    - "rpc" (default): the historian's `create_tables` RPC with `{"definitions": [...], "schema_version": N}`,
      addressed with `external_platform` when the historian runs on another platform. The call is retried every
      `rpc_retry_period` seconds until it is answered or `rpc_total_timeout` has passed.
    - "pubsub": for federations without cross-platform RPC. The definitions are published on the agent's
      `archive_definitions` key as `{"node", "request_id", "definitions", "schema_version"}`; the historian answers on
      the node's status topic (the agent's `archive_status` key, subscribed on all platforms) and the answer carrying
      that request id is awaited for `rpc_timeout` seconds. Without an answer the request is published again with a
      new id every `rpc_retry_period` seconds, and given up after `rpc_total_timeout`.

    Both paths yield the same reply: status "success" (archive on), "partial" (archive on; records for the rejected
    tables are dropped, each table reported once) or "error" (raised; nothing is archived). The historian refuses a
    node whose archive schema version is not its own TENT's with an "error" naming both versions; the reply carries
    the historian's version as `schema_version`, which is logged.

    Records are never published one by one: archive_data only queues them. Once the archive is on, the start-up backlog
    is published and a flusher greenlet publishes whatever has queued every `publish_interval` seconds, as one message
    per flush split into chunks of at most `max_batch_records` records, in order. A failed publish is logged and the
    records go back to the front of the queue for the next flush. stop() ends the flusher after a final flush.
    """

    ARCHIVE_KEY = 'archive'                      # publish key the agent maps to the node's records topic
    DEFINITIONS_KEY = 'archive_definitions'      # publish key for the registration request (pubsub path)
    STATUS_KEY = 'archive_status'                # subscribe key for the historian's answers (pubsub path)
    REGISTRATION_PATHS = ('rpc', 'pubsub')

    def __init__(self, parent, historian_vip: str, registration: str = 'rpc', external_platform: str = '',
                 rpc_timeout: float = 30.0, rpc_retry_period: float = 10.0, rpc_total_timeout: float = 300.0,
                 publish_interval: float = 1.0, max_batch_records: int = 1000, archive_key: str = ARCHIVE_KEY,
                 *args, **kwargs):
        # parent is the TransactiveNodeAgent (a VOLTTRON Agent). TransactiveNode passes it positionally and, like every
        # other configured component, also passes the node as the keyword `transactive_node`; the base class accepts it.
        super(VolttronDataManager, self).__init__(parent, *args, **kwargs)
        if not historian_vip:
            raise ValueError('VolttronDataManager requires historian_vip, the VIP identity of the TNS historian.')
        if registration not in self.REGISTRATION_PATHS:
            raise ValueError(f'VolttronDataManager registration must be one of {self.REGISTRATION_PATHS}, '
                             f'not {registration!r}.')
        self.historian_vip: str = historian_vip
        self.registration: str = registration
        self.external_platform: str = str(external_platform) if external_platform else ''
        self.rpc_timeout: float = float(rpc_timeout)
        self.rpc_retry_period: float = float(rpc_retry_period)
        self.rpc_total_timeout: float = float(rpc_total_timeout)
        self.publish_interval: float = float(publish_interval)
        self.max_batch_records: int = int(max_batch_records)
        if self.publish_interval <= 0 or self.max_batch_records < 1:
            raise ValueError('publish_interval must be positive and max_batch_records at least 1.')
        self.archive_key: str = archive_key
        self._flusher = None          # greenlet publishing the queue every publish_interval once the archive is on
        self._flusher_stopped = False
        self.rejected_tables: dict[str, Mapping] = {}   # table name -> {"message", "differences"} from the historian
        self._dropped_tables_reported: set[str] = set()
        self._pending_request_id: str | None = None
        self._pending_reply: AsyncResult | None = None

    @property
    def historian_address(self) -> str:
        return f'{self.historian_vip}@{self.external_platform}' if self.external_platform else self.historian_vip

    def _call_historian(self, method: str, *args):
        kwargs = {'external_platform': self.external_platform} if self.external_platform else {}
        return self.parent.vip.rpc.call(self.historian_vip, method, *args, **kwargs).get(timeout=self.rpc_timeout)

    def init_archive(self):
        """Register this node's tables with the historian, then release whatever was queued while waiting."""
        definitions = list(self.definitions.values()) if isinstance(self.definitions, Mapping) else list(self.definitions)
        _log.info(f'Registering {len(definitions)} archive tables with historian {self.historian_address!r} '
                  f'by {self.registration}.')
        if self.registration == 'pubsub':
            self.parent.register_subscription(self.STATUS_KEY, self._receive_status, all_platforms=True)
        deadline = time.monotonic() + self.rpc_total_timeout
        attempt = 0
        while True:
            attempt += 1
            try:
                if self.registration == 'pubsub':
                    reply = self._request_registration_by_pubsub(definitions)
                else:
                    reply = self._call_historian('create_tables', {'definitions': definitions,
                                                                   'schema_version': ARCHIVE_SCHEMA_VERSION})
                break
            except Exception as error:  # unreachable peer, no federation route yet, or no reply in time
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise RuntimeError(f'Historian {self.historian_address!r} did not answer the table registration in '
                                       f'{self.rpc_total_timeout:.0f} s ({attempt} attempts); nothing will be archived '
                                       f'in this run.') from error
                _log.error(f'Table registration with historian {self.historian_address!r} failed (attempt {attempt}: '
                           f'{error!r}); retrying in {self.rpc_retry_period:.0f} s for up to {remaining:.0f} s more.')
                gevent.sleep(self.rpc_retry_period)
        self._apply_registration_reply(reply, attempt)
        self.flush()
        self._flusher_stopped = False
        self._flusher = gevent.spawn(self._flush_periodically)

    def _request_registration_by_pubsub(self, definitions: list) -> Mapping:
        """Publish one registration request and wait rpc_timeout seconds for the status that answers it."""
        request_id = str(uuid4())
        self._pending_request_id = request_id
        self._pending_reply = AsyncResult()
        self.parent.publish(self.DEFINITIONS_KEY, message={'node': self.parent.name, 'request_id': request_id,
                                                           'definitions': definitions,
                                                           'schema_version': ARCHIVE_SCHEMA_VERSION})
        try:
            return self._pending_reply.get(timeout=self.rpc_timeout)
        except gevent.Timeout:
            raise TimeoutError(f'no status for request {request_id} within {self.rpc_timeout:.0f} s')
        finally:
            self._pending_request_id = None
            self._pending_reply = None

    @property
    def pending_request_id(self) -> str | None:
        return self._pending_request_id

    def _receive_status(self, time_received, subscribe_key, message):
        """Subscription callback for the historian's status topic. Only the answer to the pending request is taken."""
        if not isinstance(message, Mapping):
            _log.warning(f'Ignoring a status message that is not a mapping: {message!r}')
            return
        request_id = message.get('request_id')
        if self._pending_reply is None or request_id != self._pending_request_id:
            _log.debug(f'Ignoring status for request {request_id!r}; pending request is {self._pending_request_id!r}.')
            return
        self._pending_reply.set(message)

    def _apply_registration_reply(self, reply, attempts: int):
        if not isinstance(reply, Mapping):
            raise RuntimeError(f'Historian {self.historian_address!r} answered the table registration with '
                               f'{reply!r}, not a status mapping.')
        status = reply.get('status')
        rejected = reply.get('rejected_tables') or {}
        if status in ('success', 'partial'):
            _log.info(f'Historian {self.historian_address!r} writes archive schema version '
                      f'{reply.get("schema_version")!r}; this node writes version {ARCHIVE_SCHEMA_VERSION}.')
        if status == 'success':
            _log.info(f'Historian {self.historian_address!r} registered the archive tables after {attempts} attempt(s).')
        elif status == 'partial':
            for table, detail in rejected.items():
                detail = detail if isinstance(detail, Mapping) else {'message': str(detail), 'differences': None}
                self.rejected_tables[table] = detail
                _log.error(f'Historian {self.historian_address!r} rejected archive table {table!r}: '
                           f'{detail.get("message")}; differences: {detail.get("differences")}. '
                           f'Records for this table will be dropped.')
            _log.warning(f'Historian {self.historian_address!r} registered the archive tables except '
                         f'{sorted(self.rejected_tables)} after {attempts} attempt(s).')
        elif status == 'error':
            # Includes the historian refusing this node's archive schema version; its message names both versions.
            raise RuntimeError(f'Historian {self.historian_address!r} did not register the archive tables: '
                               f'{reply.get("message")!r}; rejected tables: {dict(rejected)!r}; historian archive '
                               f'schema version {reply.get("schema_version")!r}, this node\'s {ARCHIVE_SCHEMA_VERSION}')
        else:
            raise RuntimeError(f'Historian {self.historian_address!r} answered the table registration with an unknown '
                               f'status {status!r}: {reply!r}')
        self.archive_ready = True

    def archive_data(self, records: Mapping | Sequence | None = None):
        """Queue records for the flusher. Records for tables the historian rejected are dropped, with one error per
        table. Nothing is published here."""
        if records is None:
            return
        self.archival_queue.extend(r for r in ([records] if isinstance(records, Mapping) else records)
                                   if self._accepted(r))

    def _accepted(self, record) -> bool:
        """False for a record addressed to a table the historian rejected; the first drop per table is an error log."""
        table = record.get('table_name') if isinstance(record, Mapping) else None
        if table not in self.rejected_tables:
            return True
        if table not in self._dropped_tables_reported:
            self._dropped_tables_reported.add(table)
            _log.error(f'Dropping records for archive table {table!r}, which historian {self.historian_address!r} '
                       f'rejected: {self.rejected_tables[table].get("message")}')
        return False

    def flush(self):
        """Publish everything queued, in order, as messages of at most max_batch_records records each. Nothing goes out
        before the historian has the tables. If a publish fails, its records and the rest return to the front of the
        queue for the next flush; the failure is logged, not raised."""
        if not self.archive_ready or not self.archival_queue:
            return
        # Records queued before the historian answered are filtered here; later ones were filtered as they arrived.
        pending = [r for r in self.archival_queue if self._accepted(r)]
        self.archival_queue.clear()
        if not pending:
            return
        _log.debug(f'Publishing {len(pending)} archive records in chunks of {self.max_batch_records}.')
        for start in range(0, len(pending), self.max_batch_records):
            chunk = pending[start:start + self.max_batch_records]
            try:
                self.parent.publish(self.archive_key, message=chunk)
            except (TypeError, ValueError):
                # A record that cannot be serialised (a NaN or infinite number, an unknown type) would fail on every
                # flush; requeueing it would stop the archive for good. Drop the chunk and go on.
                _log.exception(f'{len(chunk)} archive records cannot be serialised and are dropped: '
                               f'{sorted({r.get("table_name") for r in chunk if isinstance(r, Mapping)})}')
            except Exception:
                _log.exception(f'Publishing {len(chunk)} archive records failed; they are requeued for the next flush.')
                self.archival_queue.extendleft(reversed(pending[start:]))
                return

    def _flush_periodically(self):
        while not self._flusher_stopped:
            gevent.sleep(self.publish_interval)
            self.flush()

    def stop(self):
        """End the periodic flushing and publish what is still queued. Called when the node is torn down."""
        self._flusher_stopped = True
        flusher, self._flusher = self._flusher, None
        if flusher is not None:
            flusher.kill()
        self.flush()
