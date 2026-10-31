"""VolttronDataManager against a fake agent: table registration over RPC, queueing until the historian is ready,
publishing envelopes through the agent's archive key. Runs without a VOLTTRON platform."""
from collections import deque

import pytest

from transactive_node.data_manager import VolttronDataManager


class FakeResult:
    def __init__(self, value):
        self.value = value

    def get(self, timeout=None):
        self.timeout = timeout
        return self.value


class FakeRpc:
    def __init__(self, reply):
        self.reply = reply
        self.calls = []

    def call(self, peer, method, *args, **kwargs):
        self.calls.append((peer, method, args, kwargs))
        return FakeResult(self.reply)


class FakeAgent:
    """What the data manager needs of a TransactiveNodeAgent: vip.rpc and publish(key, message)."""
    def __init__(self, reply=None):
        self.vip = type('Vip', (), {})()
        self.vip.rpc = FakeRpc(reply if reply is not None else {'status': 'success', 'registered_tables': []})
        self.published = []

    def publish(self, publish_key, message, date_time=None):
        self.published.append((publish_key, message))


DEFINITION = {"name": "test_table", "columns": [{"name": "id", "type": "Uuid", "kwargs": {"primary_key": True}},
                                                 {"name": "value", "type": "Float"}]}


def test_requires_a_historian_identity():
    with pytest.raises(ValueError):
        VolttronDataManager(FakeAgent(), historian_vip='')


def test_accepts_the_node_keyword_the_configuration_path_passes():
    agent = FakeAgent()
    dm = VolttronDataManager(agent, historian_vip='platform.historian', transactive_node=agent)
    assert dm.historian_vip == 'platform.historian' and dm.archive_key == 'archive'


def test_records_queue_until_tables_are_registered_then_publish_as_one_batch():
    agent = FakeAgent()
    dm = VolttronDataManager(agent, historian_vip='platform.historian', rpc_timeout=5)
    dm.add_tables([DEFINITION])
    dm.archive_data({'table_name': 'test_table', 'data': {'id': 'a', 'value': 1.0}})
    dm.archive_data([{'table_name': 'test_table', 'data': {'id': 'b', 'value': 2.0}}])
    assert agent.published == [] and len(dm.archival_queue) == 2

    dm.init_archive()

    peer, method, args, _ = agent.vip.rpc.calls[0]
    assert (peer, method) == ('platform.historian', 'create_tables')
    assert args[0] == [DEFINITION], 'the table definitions, as a list'
    assert dm.archive_ready
    assert len(agent.published) == 1
    key, batch = agent.published[0]
    assert key == 'archive' and [r['data']['id'] for r in batch] == ['a', 'b']
    assert len(dm.archival_queue) == 0

    dm.archive_data([{'table_name': 'test_table', 'data': {'id': 'c', 'value': 3.0}}])
    assert agent.published[1][1][0]['data']['id'] == 'c', 'once ready, each batch is published immediately'


def test_historian_error_is_not_hidden():
    agent = FakeAgent(reply={'status': 'error', 'message': 'no such type'})
    dm = VolttronDataManager(agent, historian_vip='platform.historian')
    dm.add_tables([DEFINITION])
    with pytest.raises(RuntimeError):
        dm.init_archive()
    assert not dm.archive_ready and agent.published == []
