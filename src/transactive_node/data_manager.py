"""
VolttronDataManager: the DataManager a TransactiveNodeAgent uses to archive through a VOLTTRON historian.

TENT itself knows nothing about VOLTTRON. Its components hand archive records to whatever DataManager the node was
configured with; this one forwards them over the message bus to the transactive-node historian
(volttron-transactive-node-historian), which owns the database. Two interactions:

- At start-up (``init_archive``) the table definitions every configured component declared are sent to the historian
  with the ``create_tables`` RPC, and the reply is waited for. Records queued before that moment are released once the
  historian has acknowledged the tables. If the historian does not answer, the error is raised rather than hidden:
  nothing would be archived in that run and the operator must know.
- Every batch of records (``archive_data``) is published as a list of ``{"table_name": ..., "data": {...}}`` envelopes
  on the agent's archive topic (``<db_topic>/<node name>/archive``), which the historian captures. Values are the
  JSON forms ``tent.utils.helpers.json_encoder`` produces (UUIDs and enumeration names as strings, datetimes as ISO
  8601, durations as seconds); the historian's LocalDataManager parses them back by column type.

Configuration (the ``data_manager`` block of the node config)::

    {"class_name": "VolttronDataManager", "module_name": "transactive_node.data_manager",
     "historian_vip": "platform.historian", "rpc_timeout": 30}

``historian_vip`` must be the VIP identity the historian is installed under; it has no default because the two sides
have disagreed before and a silent mismatch simply archives nothing.
"""
import logging
from collections.abc import Mapping, Sequence

from tent.data_manager import DataManager

_log = logging.getLogger(__name__)


class VolttronDataManager(DataManager):
    ARCHIVE_KEY = 'archive'  # publish key the agent maps to its archive topic

    def __init__(self, parent, historian_vip: str, rpc_timeout: float = 30.0, archive_key: str = ARCHIVE_KEY,
                 *args, **kwargs):
        # parent is the TransactiveNodeAgent (a VOLTTRON Agent). TransactiveNode passes it positionally and, like every
        # other configured component, also passes the node as the keyword `transactive_node`; the base class accepts it.
        super(VolttronDataManager, self).__init__(parent, *args, **kwargs)
        if not historian_vip:
            raise ValueError('VolttronDataManager requires historian_vip, the VIP identity of the TNS historian.')
        self.historian_vip: str = historian_vip
        self.rpc_timeout: float = float(rpc_timeout)
        self.archive_key: str = archive_key

    def init_archive(self):
        """Register this node's tables with the historian, then release whatever was queued while waiting."""
        definitions = list(self.definitions.values()) if isinstance(self.definitions, Mapping) else list(self.definitions)
        _log.info(f'Registering {len(definitions)} archive tables with historian {self.historian_vip!r}.')
        reply = self.parent.vip.rpc.call(self.historian_vip, 'create_tables', definitions).get(timeout=self.rpc_timeout)
        if not isinstance(reply, Mapping) or reply.get('status') != 'success':
            raise RuntimeError(f'Historian {self.historian_vip!r} did not register the archive tables: {reply!r}')
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
