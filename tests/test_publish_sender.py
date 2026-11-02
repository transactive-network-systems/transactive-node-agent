"""publish() queues; one sender greenlet publishes in order, so the caller never waits on the platform's round trips."""
import logging
import time

import pytest

import transactive_node.agent as agent_module
from transactive_node.agent import TransactiveNodeAgent


class SlowPubSub:
    """A pubsub whose publish takes `delay` seconds (or fails for one topic) and records the order of publications."""
    def __init__(self, delay=0.0, failing_topic=None):
        self.delay, self.failing_topic = delay, failing_topic
        self.published = []
        self.subscriptions, self.unsubscribed = [], []

    def publish(self, peer, topic, headers=None, message=None, **kwargs):
        time.sleep(self.delay)
        if topic == self.failing_topic:
            raise RuntimeError('bus refused')
        self.published.append((topic, headers, message))

    def subscribe(self, *args, **kwargs):
        pass


@pytest.fixture
def agent():
    agent = TransactiveNodeAgent()
    agent.topics = {'SIG': 'tns/Campus/CoR', 'SIG2': 'tns/Campus/Office', 'archive': 'tns/archive/Campus/records'}
    return agent


def test_the_caller_returns_before_the_platform_publish_and_the_sender_keeps_the_order(agent):
    agent.vip.pubsub = SlowPubSub(delay=0.01)
    started = time.monotonic()
    for i in range(30):
        agent.publish(['SIG', 'archive', 'SIG2'][i % 3], {'n': i})
    assert time.monotonic() - started < 0.1, 'thirty slow publishes returned at once'
    assert agent.vip.pubsub.published == [] and len(agent._publish_queue) == 30
    assert agent._sender is not None and agent._sender.func == agent._send_loop and agent._publish_wake.is_set()

    agent._send_pending()  # what the sender greenlet does when woken
    assert [m['n'] for _, _, m in agent.vip.pubsub.published] == list(range(30)), 'in order, across topics'
    assert [t for t, _, _ in agent.vip.pubsub.published][:3] == ['tns/Campus/CoR', 'tns/archive/Campus/records',
                                                                 'tns/Campus/Office']
    assert all(set(h) == {'Date', 'TimeStamp'} for _, h, _ in agent.vip.pubsub.published)
    assert len(agent._publish_queue) == 0


def test_a_failing_publish_is_logged_with_its_topic_and_the_next_one_still_goes_out(agent, caplog):
    agent.vip.pubsub = SlowPubSub(failing_topic='tns/Campus/CoR')
    agent.publish('SIG', {'n': 1})
    agent.publish('SIG2', {'n': 2})
    with caplog.at_level(logging.ERROR, logger='transactive_node.agent'):
        agent._send_pending()
    errors = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert len(errors) == 1 and "'tns/Campus/CoR'" in errors[0].getMessage() and errors[0].exc_info is not None
    assert [m['n'] for _, _, m in agent.vip.pubsub.published] == [2]


def test_stop_drains_the_queue_and_ends_the_sender(agent):
    agent.vip.pubsub = SlowPubSub()
    agent.publish('SIG', {'n': 1})
    agent.publish('SIG', {'n': 2})
    sender = agent._sender
    agent.onstop(None)
    assert sender.dead and agent._sender is None and agent._sender_stopped
    assert [m['n'] for _, _, m in agent.vip.pubsub.published] == [1, 2]
    agent.publish('SIG', {'n': 3})
    assert agent._sender is None, 'no sender is started after stop'


def test_a_non_serialisable_message_still_raises_to_the_caller(agent):
    with pytest.raises(ValueError):
        agent.publish('SIG', {'price': float('inf')})
    assert len(agent._publish_queue) == 0


def test_queue_depth_is_warned_about_once_per_period_and_the_drain_is_logged(agent, caplog, monkeypatch):
    clock = {'now': 500.0}
    monkeypatch.setattr(agent_module.time, 'monotonic', lambda: clock['now'])
    agent.vip.pubsub = SlowPubSub()
    with caplog.at_level(logging.DEBUG, logger='transactive_node.agent'):
        for i in range(25):
            agent.publish('SIG', {'n': i})
        clock['now'] += 61
        agent.publish('SIG', {'n': 25})
        agent._send_pending()
    warnings = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 2, 'one at the 21st message, one more after the period, not one per message'
    assert warnings[0].startswith('21 publications are waiting') and warnings[1].startswith('26 publications')
    assert any(r.levelno == logging.DEBUG and 'drained' in r.getMessage() for r in caplog.records)
    assert not agent._publish_queue_warned


def test_the_slow_publish_warning_is_measured_in_the_sender(agent, caplog, monkeypatch):
    clock = {'now': 100.0}
    monkeypatch.setattr(agent_module.time, 'monotonic', lambda: clock['now'])
    pubsub = SlowPubSub()
    original = pubsub.publish

    def slow_publish(*args, **kwargs):
        clock['now'] += 2.5
        original(*args, **kwargs)
    pubsub.publish = slow_publish
    agent.vip.pubsub = pubsub
    agent.publish('SIG', {})
    with caplog.at_level(logging.WARNING, logger='transactive_node.agent'):
        agent._send_pending()
    assert any('took 2.5 s' in r.getMessage() for r in caplog.records)


def test_the_send_loop_waits_publishes_and_stops(agent):
    agent.vip.pubsub = SlowPubSub()
    agent.publish('SIG', {'n': 1})
    calls = {'waits': 0}
    original_wait = agent._publish_wake.wait

    def wait(timeout=None):
        calls['waits'] += 1
        if calls['waits'] == 2:
            agent._sender_stopped = True
        return original_wait(timeout)
    agent._publish_wake.wait = wait
    agent._send_loop()
    assert [m['n'] for _, _, m in agent.vip.pubsub.published] == [1] and not agent._publish_wake.is_set()
