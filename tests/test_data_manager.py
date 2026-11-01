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


class FlakyRpc(FakeRpc):
    """Fails the first `failures` calls the way an unreachable peer does, then answers."""
    def __init__(self, reply, failures):
        super().__init__(reply)
        self.failures = failures

    def call(self, peer, method, *args, **kwargs):
        self.calls.append((peer, method, args, kwargs))
        if len(self.calls) <= self.failures:
            raise TimeoutError('no reply')
        return FakeResult(self.reply)


def test_remote_historian_is_addressed_with_its_platform():
    agent = FakeAgent()
    dm = VolttronDataManager(agent, historian_vip='tns.historian', external_platform='central')
    dm.add_tables([DEFINITION])
    dm.init_archive()
    peer, method, _, kwargs = agent.vip.rpc.calls[0]
    assert (peer, method, kwargs) == ('tns.historian', 'create_tables', {'external_platform': 'central'})
    assert dm.historian_address == 'tns.historian@central'


def test_local_historian_is_called_without_a_platform():
    agent = FakeAgent()
    dm = VolttronDataManager(agent, historian_vip='platform.historian')
    dm.add_tables([DEFINITION])
    dm.init_archive()
    assert agent.vip.rpc.calls[0][3] == {}


def test_unanswered_registration_is_retried_until_the_historian_answers(monkeypatch):
    import transactive_node.data_manager as module
    sleeps = []
    monkeypatch.setattr(module.gevent, 'sleep', lambda seconds: sleeps.append(seconds))
    agent = FakeAgent()
    agent.vip.rpc = FlakyRpc({'status': 'success', 'registered_tables': []}, failures=2)
    dm = VolttronDataManager(agent, historian_vip='tns.historian', external_platform='central', rpc_retry_period=7,
                             rpc_total_timeout=600)
    dm.add_tables([DEFINITION])
    dm.init_archive()
    assert len(agent.vip.rpc.calls) == 3 and sleeps == [7, 7]
    assert dm.archive_ready


def test_registration_gives_up_after_the_total_timeout(monkeypatch):
    import transactive_node.data_manager as module
    clock = {'now': 0.0}
    monkeypatch.setattr(module.gevent, 'sleep', lambda seconds: clock.__setitem__('now', clock['now'] + seconds))
    monkeypatch.setattr(module.time, 'monotonic', lambda: clock['now'])
    agent = FakeAgent()
    agent.vip.rpc = FlakyRpc({'status': 'success'}, failures=100)
    dm = VolttronDataManager(agent, historian_vip='tns.historian', rpc_retry_period=10, rpc_total_timeout=25)
    dm.add_tables([DEFINITION])
    with pytest.raises(RuntimeError, match='did not answer create_tables'):
        dm.init_archive()
    assert len(agent.vip.rpc.calls) == 4, 'attempts at t=0, 10, 20 and the one that finds the deadline passed'
    assert not dm.archive_ready


def test_rejected_definition_is_not_retried():
    agent = FakeAgent(reply={'status': 'error', 'message': 'mismatch'})
    dm = VolttronDataManager(agent, historian_vip='tns.historian', rpc_retry_period=1)
    dm.add_tables([DEFINITION])
    with pytest.raises(RuntimeError, match='did not register'):
        dm.init_archive()
    assert len(agent.vip.rpc.calls) == 1
