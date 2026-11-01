"""VolttronDataManager against a fake agent: table registration over RPC, queueing until the historian is ready,
publishing envelopes through the agent's archive key. Runs without a VOLTTRON platform."""
import logging
import uuid

import pytest

from transactive_node.data_manager import VolttronDataManager
from tent.data_manager import ARCHIVE_SCHEMA_VERSION


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
    """What the data manager needs of a TransactiveNodeAgent: name, vip.rpc, publish(key, message) and
    register_subscription(key, callback, all_platforms). `historian` is an optional function of the published
    definitions message returning the status the historian would publish (None for no answer); it is delivered to the
    status subscription at publish time, as a historian answering on the bus would."""
    def __init__(self, reply=None, historian=None):
        self.name = 'Campus'
        self.vip = type('Vip', (), {})()
        self.vip.rpc = FakeRpc(reply if reply is not None else {'status': 'success', 'registered_tables': []})
        self.published = []
        self.subscriptions = []  # (key, callback, all_platforms)
        self.historian = historian

    def publish(self, publish_key, message, date_time=None):
        self.published.append((publish_key, message))
        if publish_key == 'archive_definitions' and self.historian is not None:
            status = self.historian(message)
            if status is not None:
                self.deliver_status(status)

    def register_subscription(self, subscribe_key, callback, all_platforms=None):
        self.subscriptions.append((subscribe_key, callback, all_platforms))

    def deliver_status(self, status):
        for key, callback, _ in self.subscriptions:
            if key == 'archive_status':
                callback(None, key, status)

    def definitions_published(self):
        return [m for k, m in self.published if k == 'archive_definitions']

    def records_published(self):
        return [m for k, m in self.published if k == 'archive']


DEFINITION = {"name": "test_table", "columns": [{"name": "id", "type": "Uuid", "kwargs": {"primary_key": True}},
                                                 {"name": "value", "type": "Float"}]}
OTHER = {"name": "other_table", "columns": [{"name": "id", "type": "Uuid", "kwargs": {"primary_key": True}}]}


def record(table, ident):
    return {'table_name': table, 'data': {'id': ident}}


def success_for(message):
    return {'request_id': message['request_id'], 'publisher': message['publisher'], 'status': 'success',
            'registered_tables': [d['name'] for d in message['definitions']], 'rejected_tables': {}, 'message': ''}


def partial_for(message):
    return {'request_id': message['request_id'], 'publisher': message['publisher'], 'status': 'partial',
            'registered_tables': ['test_table'],
            'rejected_tables': {'other_table': {'message': 'definition differs', 'differences': 'column id: Uuid vs Int'}},
            'message': 'one table rejected'}


def error_for(message):
    return {'request_id': message['request_id'], 'publisher': message['publisher'], 'status': 'error', 'registered_tables': [],
            'rejected_tables': {'test_table': {'message': 'no such type', 'differences': None}}, 'message': 'bad types'}


def test_requires_a_historian_identity():
    with pytest.raises(ValueError):
        VolttronDataManager(FakeAgent(), historian_vip='')


def test_configuration_is_validated_by_the_config_model():
    from tent.config import ConfigurationError
    agent = FakeAgent()
    dm = VolttronDataManager(agent, historian_vip='platform.historian')
    assert dm.historian_vip == 'platform.historian' and dm.archive_key == 'archive' and dm.config_version == 1
    assert dm.config.model_dump()['registration'] == 'rpc'
    with pytest.raises(ConfigurationError, match="VolttronDataManager: unknown configuration key 'transactive_node'"):
        VolttronDataManager(agent, historian_vip='platform.historian', transactive_node=agent)
    assert VolttronDataManager.Config.reconfigurable_fields() == {'rpc_timeout', 'rpc_retry_period',
                                                                  'rpc_total_timeout', 'publish_interval',
                                                                  'max_batch_records'}


def test_timeouts_are_reconfigurable_in_place_and_the_registration_path_is_not():
    from tent.config import ReconfigurationError
    agent = FakeAgent()
    dm = ready_dm(agent, publish_interval=1.0)
    dm.reconfigure({'publish_interval': 2.5, 'rpc_timeout': 3.0})
    assert (dm.publish_interval, dm.rpc_timeout, dm.config_version) == (2.5, 3.0, 2)
    with pytest.raises(ReconfigurationError):
        dm.reconfigure({'registration': 'pubsub'})
    assert dm.registration == 'rpc'


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
    assert args[0] == {'definitions': [DEFINITION], 'schema_version': ARCHIVE_SCHEMA_VERSION}, \
        'the table definitions beside this TENT\'s archive schema version'
    assert dm.archive_ready
    assert len(agent.published) == 1
    key, batch = agent.published[0]
    assert key == 'archive' and [r['data']['id'] for r in batch] == ['a', 'b']
    assert len(dm.archival_queue) == 0

    dm.archive_data([{'table_name': 'test_table', 'data': {'id': 'c', 'value': 3.0}}])
    assert len(agent.published) == 1, 'once ready, records wait for the next flush'
    dm.flush()
    assert agent.published[1][1][0]['data']['id'] == 'c'
    dm.stop()


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
    with pytest.raises(RuntimeError, match='did not answer the table registration'):
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


# ----------------------------------------------------------------------------------------------------------------------
# Registration path selection and the new reply shape on the RPC path
# ----------------------------------------------------------------------------------------------------------------------

def test_registration_must_be_rpc_or_pubsub():
    with pytest.raises(ValueError, match='registration'):
        VolttronDataManager(FakeAgent(), historian_vip='tns.historian', registration='http')
    assert VolttronDataManager(FakeAgent(), historian_vip='tns.historian').registration == 'rpc'


def test_rpc_path_accepts_the_full_status_shape_and_does_not_subscribe():
    agent = FakeAgent(reply={'request_id': None, 'publisher': 'Campus', 'status': 'success',
                             'registered_tables': ['test_table'], 'rejected_tables': {}, 'message': ''})
    dm = VolttronDataManager(agent, historian_vip='tns.historian')
    dm.add_tables([DEFINITION])
    dm.archive_data(record('test_table', 'a'))
    dm.init_archive()
    assert dm.archive_ready and agent.subscriptions == [] and agent.definitions_published() == []
    assert agent.records_published() == [[record('test_table', 'a')]]


def test_rpc_partial_registers_and_drops_records_for_rejected_tables(caplog):
    agent = FakeAgent(reply=partial_for({'request_id': None, 'publisher': 'Campus', 'definitions': []}))
    dm = VolttronDataManager(agent, historian_vip='tns.historian')
    dm.add_tables([DEFINITION, OTHER])
    dm.archive_data([record('test_table', 'a'), record('other_table', 'x')])
    with caplog.at_level(logging.ERROR, logger='transactive_node.data_manager'):
        dm.init_archive()
        dm.archive_data([record('other_table', 'y'), record('test_table', 'b'), record('other_table', 'z')])
        dm.flush()
    assert dm.archive_ready and set(dm.rejected_tables) == {'other_table'}
    assert agent.records_published() == [[record('test_table', 'a')], [record('test_table', 'b')]]
    errors = [r.getMessage() for r in caplog.records if r.levelno == logging.ERROR]
    assert sum('rejected archive table' in m and 'column id: Uuid vs Int' in m for m in errors) == 1
    assert sum('Dropping records for archive table' in m for m in errors) == 1, 'once per table, not per record'


def test_rpc_error_reply_raises_with_the_details():
    agent = FakeAgent(reply=error_for({'request_id': None, 'publisher': 'Campus', 'definitions': []}))
    dm = VolttronDataManager(agent, historian_vip='tns.historian')
    dm.add_tables([DEFINITION])
    dm.archive_data(record('test_table', 'a'))
    with pytest.raises(RuntimeError, match="bad types.*no such type"):
        dm.init_archive()
    assert not dm.archive_ready and agent.published == []


def test_schema_version_refusal_raises_naming_both_versions():
    """What the historian answers when this node's TENT writes another archive schema version than its own."""
    refusal = {'status': 'error', 'registered_tables': [], 'rejected_tables': {},
               'message': f'The node states archive schema version {ARCHIVE_SCHEMA_VERSION}; this historian\'s TENT '
                          f'writes archive schema version {ARCHIVE_SCHEMA_VERSION + 1}. Nothing was registered.',
               'schema_version': ARCHIVE_SCHEMA_VERSION + 1}
    agent = FakeAgent(reply=refusal)
    dm = VolttronDataManager(agent, historian_vip='tns.historian')
    dm.add_tables([DEFINITION])
    dm.archive_data(record('test_table', 'a'))
    with pytest.raises(RuntimeError) as error:
        dm.init_archive()
    message = str(error.value)
    assert f'version {ARCHIVE_SCHEMA_VERSION + 1}' in message and f'version {ARCHIVE_SCHEMA_VERSION}' in message
    assert 'Nothing was registered' in message
    assert not dm.archive_ready and agent.published == []


def test_historians_schema_version_is_logged_on_success(caplog):
    agent = FakeAgent(reply={'status': 'success', 'registered_tables': ['test_table'], 'rejected_tables': {},
                             'message': '', 'schema_version': ARCHIVE_SCHEMA_VERSION})
    dm = VolttronDataManager(agent, historian_vip='tns.historian')
    dm.add_tables([DEFINITION])
    with caplog.at_level(logging.INFO, logger='transactive_node.data_manager'):
        dm.init_archive()
    assert any(f'writes archive schema version {ARCHIVE_SCHEMA_VERSION}' in r.getMessage() for r in caplog.records)


def test_only_records_without_rejected_tables_are_published_when_a_batch_is_all_rejected():
    agent = FakeAgent(reply=partial_for({'request_id': None, 'publisher': 'Campus', 'definitions': []}))
    dm = VolttronDataManager(agent, historian_vip='tns.historian')
    dm.add_tables([DEFINITION, OTHER])
    dm.init_archive()
    dm.archive_data([record('other_table', 'x')])
    dm.flush()
    assert agent.records_published() == [], 'no empty batch goes out'


# ----------------------------------------------------------------------------------------------------------------------
# Pubsub registration path
# ----------------------------------------------------------------------------------------------------------------------

def make_pubsub_dm(agent, **kwargs):
    dm = VolttronDataManager(agent, historian_vip='tns.historian', registration='pubsub', external_platform='central',
                             rpc_timeout=5, **kwargs)
    dm.add_tables([DEFINITION, OTHER])
    return dm


def test_pubsub_success_subscribes_everywhere_publishes_definitions_and_flushes():
    agent = FakeAgent(historian=success_for)
    dm = make_pubsub_dm(agent)
    dm.archive_data(record('test_table', 'a'))
    dm.init_archive()

    assert agent.subscriptions == [('archive_status', dm._receive_status, True)]
    assert agent.vip.rpc.calls == [], 'no RPC on the pubsub path'
    (request,) = agent.definitions_published()
    assert request['publisher'] == 'Campus' and request['definitions'] == [DEFINITION, OTHER]
    assert request['schema_version'] == ARCHIVE_SCHEMA_VERSION
    uuid.UUID(request['request_id'], version=4)
    assert dm.archive_ready and agent.records_published() == [[record('test_table', 'a')]]
    assert dm.pending_request_id is None


def test_pubsub_partial_and_error_follow_the_same_rules_as_rpc(caplog):
    agent = FakeAgent(historian=partial_for)
    dm = make_pubsub_dm(agent)
    with caplog.at_level(logging.ERROR, logger='transactive_node.data_manager'):
        dm.init_archive()
        dm.archive_data([record('other_table', 'x'), record('test_table', 'a')])
        dm.flush()
    assert dm.archive_ready and set(dm.rejected_tables) == {'other_table'}
    assert agent.records_published() == [[record('test_table', 'a')]]
    assert any('definition differs' in r.getMessage() for r in caplog.records)

    agent = FakeAgent(historian=error_for)
    dm = make_pubsub_dm(agent)
    with pytest.raises(RuntimeError, match='bad types'):
        dm.init_archive()
    assert not dm.archive_ready and agent.records_published() == []


def test_pubsub_retries_with_a_new_request_id_and_ignores_answers_to_other_requests(monkeypatch, caplog):
    import transactive_node.data_manager as module
    sleeps = []
    monkeypatch.setattr(module.gevent, 'sleep', lambda seconds: sleeps.append(seconds))
    answers = {'count': 0}

    def historian(message):
        answers['count'] += 1
        if answers['count'] == 1:
            return None  # the first request goes unanswered
        if answers['count'] == 2:
            # A late answer to the first request arrives before the answer to the second: it must be ignored.
            first_id = agent.definitions_published()[0]['request_id']
            agent.deliver_status(dict(success_for(message), request_id=first_id))
            return None
        return success_for(message)

    agent = FakeAgent(historian=historian)
    dm = make_pubsub_dm(agent, rpc_retry_period=7, rpc_total_timeout=600)
    with caplog.at_level(logging.DEBUG, logger='transactive_node.data_manager'):
        dm.init_archive()
    requests = agent.definitions_published()
    assert len(requests) == 3 and len({r['request_id'] for r in requests}) == 3, 'a fresh id on every attempt'
    assert sleeps == [7, 7]
    assert dm.archive_ready
    errors = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert len(errors) == 2 and all('no status for request' in r.getMessage() for r in errors)
    assert any('Ignoring status for request' in r.getMessage() for r in caplog.records if r.levelno == logging.DEBUG)


def test_pubsub_gives_up_after_the_total_timeout(monkeypatch):
    import transactive_node.data_manager as module
    clock = {'now': 0.0}
    monkeypatch.setattr(module.gevent, 'sleep', lambda seconds: clock.__setitem__('now', clock['now'] + seconds))
    monkeypatch.setattr(module.time, 'monotonic', lambda: clock['now'])
    agent = FakeAgent(historian=lambda message: None)
    dm = make_pubsub_dm(agent, rpc_retry_period=10, rpc_total_timeout=25)
    with pytest.raises(RuntimeError, match='did not answer the table registration'):
        dm.init_archive()
    assert len(agent.definitions_published()) == 4 and not dm.archive_ready


def test_status_that_is_not_a_mapping_is_ignored(caplog):
    agent = FakeAgent(historian=lambda message: 'nonsense')
    dm = make_pubsub_dm(agent, rpc_retry_period=1, rpc_total_timeout=0)
    with pytest.raises(RuntimeError):
        with caplog.at_level(logging.WARNING, logger='transactive_node.data_manager'):
            dm.init_archive()
    assert any('not a mapping' in r.getMessage() for r in caplog.records)


# ----------------------------------------------------------------------------------------------------------------------
# Batched publishing on a timer
# ----------------------------------------------------------------------------------------------------------------------

def ready_dm(agent, **kwargs):
    dm = VolttronDataManager(agent, historian_vip='tns.historian', **kwargs)
    dm.add_tables([DEFINITION])
    dm.init_archive()
    return dm


def test_publish_interval_and_batch_size_are_validated():
    with pytest.raises(ValueError):
        VolttronDataManager(FakeAgent(), historian_vip='tns.historian', publish_interval=0)
    with pytest.raises(ValueError):
        VolttronDataManager(FakeAgent(), historian_vip='tns.historian', max_batch_records=0)
    dm = VolttronDataManager(FakeAgent(), historian_vip='tns.historian')
    assert (dm.publish_interval, dm.max_batch_records) == (1.0, 1000)


def test_archive_data_only_queues_and_flush_publishes_one_message_in_order():
    agent = FakeAgent()
    dm = ready_dm(agent)
    for ident in 'abc':
        dm.archive_data(record('test_table', ident))
    dm.archive_data([record('test_table', 'd'), record('test_table', 'e')])
    assert agent.records_published() == [] and len(dm.archival_queue) == 5
    dm.flush()
    assert agent.records_published() == [[record('test_table', i) for i in 'abcde']]
    assert len(dm.archival_queue) == 0
    dm.flush()
    assert len(agent.records_published()) == 1, 'an empty queue publishes nothing'


def test_flush_splits_large_queues_into_chunks_keeping_order():
    agent = FakeAgent()
    dm = ready_dm(agent, max_batch_records=4)
    dm.archive_data([record('test_table', i) for i in range(10)])
    dm.flush()
    chunks = agent.records_published()
    assert [len(c) for c in chunks] == [4, 4, 2]
    assert [r['data']['id'] for c in chunks for r in c] == list(range(10))


def test_nothing_is_published_before_ready_and_the_backlog_goes_out_once_on_init():
    agent = FakeAgent()
    dm = VolttronDataManager(agent, historian_vip='tns.historian')
    dm.add_tables([DEFINITION])
    dm.archive_data([record('test_table', 'a'), record('test_table', 'b')])
    dm.flush()
    assert agent.records_published() == [], 'flush before the historian has the tables publishes nothing'
    dm.init_archive()
    assert agent.records_published() == [[record('test_table', 'a'), record('test_table', 'b')]]
    assert dm._flusher is not None and dm._flusher.func == dm._flush_periodically, 'the flusher greenlet is started'


def test_failed_publish_requeues_at_the_front_without_raising(caplog):
    agent = FakeAgent()
    dm = ready_dm(agent, max_batch_records=2)
    failures = {'left': 1}
    original_publish = agent.publish

    def flaky_publish(publish_key, message, date_time=None):
        if publish_key == 'archive' and failures['left'] and message[0]['data']['id'] == 'c':
            failures['left'] -= 1
            raise ConnectionError('bus hiccup')
        original_publish(publish_key, message, date_time)
    agent.publish = flaky_publish
    dm.archive_data([record('test_table', i) for i in 'abcde'])
    with caplog.at_level(logging.ERROR, logger='transactive_node.data_manager'):
        dm.flush()  # a,b go out; c,d fail; c,d,e return to the queue
    assert agent.records_published() == [[record('test_table', 'a'), record('test_table', 'b')]]
    assert [r['data']['id'] for r in dm.archival_queue] == ['c', 'd', 'e']
    errors = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert len(errors) == 1 and errors[0].exc_info is not None
    dm.archive_data(record('test_table', 'f'))
    dm.flush()
    assert [r['data']['id'] for c in agent.records_published()[1:] for r in c] == ['c', 'd', 'e', 'f']


def test_the_flusher_loop_sleeps_then_flushes_until_stopped(monkeypatch):
    import transactive_node.data_manager as module
    agent = FakeAgent()
    dm = ready_dm(agent, publish_interval=2.5)
    sleeps = []

    def sleep(seconds):
        sleeps.append(seconds)
        dm.archive_data(record('test_table', len(sleeps)))
        if len(sleeps) == 3:
            dm._flusher_stopped = True
    monkeypatch.setattr(module.gevent, 'sleep', sleep)
    dm._flush_periodically()
    assert sleeps == [2.5, 2.5, 2.5]
    assert [r['data']['id'] for c in agent.records_published() for r in c] == [1, 2, 3]


def test_stop_kills_the_flusher_and_flushes_once_more():
    agent = FakeAgent()
    dm = ready_dm(agent)
    flusher = dm._flusher
    dm.archive_data(record('test_table', 'late'))
    dm.stop()
    assert flusher.dead and dm._flusher is None and dm._flusher_stopped
    assert agent.records_published() == [[record('test_table', 'late')]]
    dm.stop()  # idempotent


def test_unserialisable_records_are_dropped_not_requeued(caplog):
    agent = FakeAgent()
    dm = ready_dm(agent, max_batch_records=2)

    def publish(publish_key, message, date_time=None):
        if any(r['data']['id'] == 'bad' for r in message):
            raise ValueError('Out of range float values are not JSON compliant')
        agent.published.append((publish_key, message))
    agent.publish = publish
    dm.archive_data([record('test_table', 'a'), record('test_table', 'bad'), record('test_table', 'c')])
    with caplog.at_level(logging.ERROR, logger='transactive_node.data_manager'):
        dm.flush()
    assert agent.records_published() == [[record('test_table', 'c')]], 'the bad chunk is dropped, the rest goes out'
    assert len(dm.archival_queue) == 0
    errors = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert len(errors) == 1 and errors[0].exc_info is not None and 'dropped' in errors[0].getMessage()
