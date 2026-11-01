"""register_subscription's wrapper: exact-prefix matching, message unwrapping, timestamp handling, failure logging."""
import logging
from datetime import datetime, timezone

import pytest

from transactive_node.agent import TransactiveNodeAgent


class Recorder:
    def __init__(self, fail_with=None):
        self.calls = []
        self.fail_with = fail_with

    def __call__(self, time_received, subscribe_key, message):
        self.calls.append((time_received, subscribe_key, message))
        if self.fail_with:
            raise self.fail_with


@pytest.fixture
def agent():
    agent = TransactiveNodeAgent()
    agent.topics = {'HP1': 'devices/Campus/B/HP1', 'METERS': 'devices/Campus/B/METERS', 'SIG': 'tns/CoR/Campus'}
    return agent


def test_prefix_subscription_does_not_receive_a_longer_sibling(agent):
    hp1 = Recorder()
    agent.register_subscription('HP1', hp1)
    pubsub = agent.vip.pubsub
    assert pubsub.subscriptions[0][0] == 'devices/Campus/B/HP1', 'subscribed to the configured topic as prefix'

    pubsub.deliver('devices/Campus/B/HP10/all', [{'ZoneTemperature': 10.0}, {}])
    assert hp1.calls == [], 'HP10 matches the HP1 prefix on the bus and must be rejected by the wrapper'

    pubsub.deliver('devices/Campus/B/HP1/all', [{'ZoneTemperature': 1.0}, {}])
    pubsub.deliver('devices/Campus/B/HP1', [{'ZoneTemperature': 2.0}, {}])
    assert [m for _, _, m in hp1.calls] == [{'ZoneTemperature': 1.0}, {'ZoneTemperature': 2.0}]
    assert all(key == 'HP1' for _, key, _ in hp1.calls)


def test_device_messages_are_unwrapped_and_others_passed_through(agent):
    meters, signal = Recorder(), Recorder()
    agent.register_subscription('METERS', meters)
    agent.register_subscription('SIG', signal)
    agent.vip.pubsub.deliver('devices/Campus/B/METERS/all', [{'WholeBuildingPower': 0.0}, {'WholeBuildingPower': {}}])
    agent.vip.pubsub.deliver('tns/CoR/Campus', {'intervals': []})
    assert meters.calls[0][2] == {'WholeBuildingPower': 0.0}
    assert signal.calls[0][2] == {'intervals': []}


def test_timestamp_header_is_parsed_aware_and_missing_header_falls_back_to_timer(agent, fixed_now):
    recorder = Recorder()
    agent.register_subscription('SIG', recorder)
    stamped = datetime(2026, 10, 31, 11, 30, tzinfo=timezone.utc)
    agent.vip.pubsub.deliver('tns/CoR/Campus', {}, headers={'TimeStamp': stamped.isoformat()})
    agent.vip.pubsub.deliver('tns/CoR/Campus', {}, headers={})
    agent.vip.pubsub.deliver('tns/CoR/Campus', {}, headers=None)
    assert [t for t, _, _ in recorder.calls] == [stamped, fixed_now, fixed_now]


def test_naive_timestamp_header_is_rejected_and_logged(agent, caplog):
    recorder = Recorder()
    agent.register_subscription('SIG', recorder)
    with caplog.at_level(logging.ERROR, logger='transactive_node.agent'):
        agent.vip.pubsub.deliver('tns/CoR/Campus', {}, headers={'TimeStamp': '2026-10-31T11:30:00'})
    assert recorder.calls == []
    assert any(r.exc_info and 'timezone-aware' in str(r.exc_info[1]) for r in caplog.records)


def test_callback_failure_is_logged_with_traceback_and_does_not_propagate(agent, caplog):
    recorder = Recorder(fail_with=KeyError('WholeBuildingPower'))
    agent.register_subscription('METERS', recorder)
    with caplog.at_level(logging.ERROR, logger='transactive_node.agent'):
        agent.vip.pubsub.deliver('devices/Campus/B/METERS/all', [{}, {}])
    assert len(recorder.calls) == 1
    errors = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert len(errors) == 1 and errors[0].exc_info is not None, 'logged once, with the traceback'
    assert 'METERS' in errors[0].getMessage()


def test_unknown_key_is_not_subscribed(agent, caplog):
    with caplog.at_level(logging.WARNING, logger='transactive_node.agent'):
        agent.register_subscription('NOPE', Recorder())
    assert agent.vip.pubsub.subscriptions == [] and agent._subscriptions == []
    assert any('NOPE' in r.getMessage() for r in caplog.records)


def test_publish_refuses_non_finite_numbers_and_issue_signal_logs_it(agent, caplog):
    agent.topics['CoR_outgoing_transactive_signal'] = 'tns/Campus/CoR'
    with pytest.raises(ValueError):
        agent.publish('SIG', {'price': float('inf')})
    with pytest.raises(ValueError):
        agent.publish('SIG', [{'q': float('nan')}])
    assert agent.vip.pubsub.published == []
    with caplog.at_level(logging.ERROR, logger='transactive_node.agent'):
        agent.issue_signal('CoR', {'intervals': [{'price': float('inf')}]})
    assert agent.vip.pubsub.published == []
    errors = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert len(errors) == 1 and errors[0].exc_info is not None and 'CoR' in errors[0].getMessage()


def test_publish_encodes_tent_value_types_as_json(agent):
    from datetime import timedelta
    from uuid import uuid4
    ident = uuid4()
    agent.publish('SIG', {'id': ident, 'at': datetime(2026, 10, 31, tzinfo=timezone.utc), 'for': timedelta(hours=1),
                          'price': None})
    topic, headers, message = agent.vip.pubsub.published[0]
    assert topic == 'tns/CoR/Campus' and set(headers) == {'Date', 'TimeStamp'}
    assert message == {'id': str(ident), 'at': '2026-10-31T00:00:00+00:00', 'for': 3600.0, 'price': None}
