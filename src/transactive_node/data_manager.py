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

    def __init__(self, parent, historian_vip: str, external_platform: str = '', rpc_timeout: float = 30.0,
                 rpc_retry_period: float = 10.0, rpc_total_timeout: float = 300.0, archive_key: str = ARCHIVE_KEY,
                 *args, **kwargs):
        # parent is the TransactiveNodeAgent (a VOLTTRON Agent). TransactiveNode passes it positionally and, like every
        # other configured component, also passes the node as the keyword `transactive_node`; the base class accepts it.
        super(VolttronDataManager, self).__init__(parent, *args, **kwargs)
        if not historian_vip:
            raise ValueError('VolttronDataManager requires historian_vip, the VIP identity of the TNS historian.')
        self.historian_vip: str = historian_vip
        self.external_platform: str = str(external_platform) if external_platform else ''
        self.rpc_timeout: float = float(rpc_timeout)
        self.rpc_retry_period: float = float(rpc_retry_period)
        self.rpc_total_timeout: float = float(rpc_total_timeout)
        self.archive_key: str = archive_key

    @property
    def historian_address(self) -> str:
        return f'{self.historian_vip}@{self.external_platform}' if self.external_platform else self.historian_vip

    def _call_historian(self, method: str, *args):
        kwargs = {'external_platform': self.external_platform} if self.external_platform else {}
        return self.parent.vip.rpc.call(self.historian_vip, method, *args, **kwargs).get(timeout=self.rpc_timeout)

    def init_archive(self):
        """Register this node's tables with the historian, then release whatever was queued while waiting."""
        definitions = list(self.definitions.values()) if isinstance(self.definitions, Mapping) else list(self.definitions)
        _log.info(f'Registering {len(definitions)} archive tables with historian {self.historian_address!r}.')
        deadline = time.monotonic() + self.rpc_total_timeout
        attempt = 0
        while True:
            attempt += 1
            try:
                reply = self._call_historian('create_tables', definitions)
                break
            except Exception as error:  # unreachable peer, no federation route yet, or no reply in time
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise RuntimeError(f'Historian {self.historian_address!r} did not answer create_tables in '
                                       f'{self.rpc_total_timeout:.0f} s ({attempt} attempts); nothing will be archived '
                                       f'in this run.') from error
                _log.error(f'create_tables to historian {self.historian_address!r} failed (attempt {attempt}: '
                           f'{error!r}); retrying in {self.rpc_retry_period:.0f} s for up to {remaining:.0f} s more.')
                gevent.sleep(self.rpc_retry_period)
        if not isinstance(reply, Mapping) or reply.get('status') != 'success':
            raise RuntimeError(f'Historian {self.historian_address!r} did not register the archive tables: {reply!r}')
        _log.info(f'Historian {self.historian_address!r} registered the archive tables after {attempt} attempt(s).')
        self.archive_ready = True
        self.archive_data()

    def archive_data(self, records: Mapping | Sequence | None = None):
        """Queue records until the historian has the tables; then publish the queue and the new records as one batch."""
        if records is not None:
            self.archival_queue.extend([records] if isinstance(records, Mapping) else records)
        if not self.archive_ready or not self.archival_queue:
            return
        batch = []
        while self.archival_queue:
            batch.append(self.archival_queue.popleft())
        self.parent.publish(self.archive_key, message=batch)
