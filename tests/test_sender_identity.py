"""Sender bindings for transactive signals: the agent's `sender` key of a neighbor entry, applied by the subscription
wrapper against the identity the platform presents, with rate-limited warnings. TENT never sees the key."""
import logging
from copy import deepcopy

import pytest

import transactive_node.agent as agent_module
from transactive_node.agent import TransactiveNodeAgent
from test_configure import CONFIG

KEY = 'CoR_incoming_transactive_signal'
TOPIC = 'tns/CoR/Campus'


class Recorder:
    def __init__(self):
        self.calls = []

    def __call__(self, time_received, subscribe_key, message):
        self.calls.append((subscribe_key, message))


@pytest.fixture
def agent():
    agent = TransactiveNodeAgent()
    agent.topics = {KEY: TOPIC, 'METERS': 'devices/Campus/B/METERS'}
    return agent


def bound(agent, sender):
    agent._neighbor_senders = {'CoR': sender}
    recorder = Recorder()
    agent.register_subscription(KEY, recorder)
    return recorder


def test_the_bound_sender_is_accepted_and_another_is_dropped_with_a_warning(agent, caplog):
    recorder = bound(agent, 'tn.city@city')
    with caplog.at_level(logging.WARNING, logger='transactive_node.agent'):
        agent.vip.pubsub.deliver(TOPIC, {'n': 1}, sender='tn.city@city')
        agent.vip.pubsub.deliver(TOPIC, {'n': 2}, sender='tn.rogue@city')
        agent.vip.pubsub.deliver(TOPIC, {'n': 3}, sender='tn.city@elsewhere')
    assert recorder.calls == [(KEY, {'n': 1})]
    warnings = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 2
    assert all(TOPIC in w and "'tn.city@city'" in w for w in warnings)
    assert "'tn.rogue@city'" in warnings[0] and "'tn.city@elsewhere'" in warnings[1]


def test_the_bare_tail_of_a_qualified_binding_is_accepted_and_the_form_logged_once(agent, caplog):
    recorder = bound(agent, 'tn.city@city')
    with caplog.at_level(logging.INFO, logger='transactive_node.agent'):
        agent.vip.pubsub.deliver(TOPIC, {'n': 1}, sender='tn.city')
        agent.vip.pubsub.deliver(TOPIC, {'n': 2}, sender='tn.city')
    assert [m for _, m in recorder.calls] == [{'n': 1}, {'n': 2}]
    forms = [r for r in caplog.records if 'bare form' in r.getMessage()]
    assert len(forms) == 1 and forms[0].levelno == logging.INFO


def test_a_bare_binding_accepts_the_identity_from_any_platform_but_not_a_qualified_other(agent, caplog):
    recorder = bound(agent, 'tn.city')
    with caplog.at_level(logging.WARNING, logger='transactive_node.agent'):
        agent.vip.pubsub.deliver(TOPIC, {'n': 1}, sender='tn.city')
        agent.vip.pubsub.deliver(TOPIC, {'n': 2}, sender='tn.city@city')
    assert [m for _, m in recorder.calls] == [{'n': 1}], 'a qualified identity does not match a bare binding'
    assert len([r for r in caplog.records if r.levelno == logging.WARNING]) == 1


def test_an_unbound_neighbor_is_accepted_from_anyone(agent):
    agent._neighbor_senders = {}
    recorder = Recorder()
    agent.register_subscription(KEY, recorder)
    agent.vip.pubsub.deliver(TOPIC, {'n': 1}, sender='whoever')
    assert recorder.calls == [(KEY, {'n': 1})]


def test_non_signal_subscriptions_are_not_checked(agent):
    agent._neighbor_senders = {'CoR': 'tn.city@city'}
    recorder = Recorder()
    agent.register_subscription('METERS', recorder)
    agent.vip.pubsub.deliver('devices/Campus/B/METERS/all', [{'P': 1.0}, {}], sender='platform.driver')
    assert len(recorder.calls) == 1


def test_mismatch_warnings_are_rate_limited_per_sender(agent, caplog, monkeypatch):
    clock = {'now': 1000.0}
    monkeypatch.setattr(agent_module.time, 'monotonic', lambda: clock['now'])
    recorder = bound(agent, 'tn.city@city')
    with caplog.at_level(logging.WARNING, logger='transactive_node.agent'):
        for _ in range(5):
            agent.vip.pubsub.deliver(TOPIC, {}, sender='tn.rogue')
        agent.vip.pubsub.deliver(TOPIC, {}, sender='tn.other')
        clock['now'] += 30
        agent.vip.pubsub.deliver(TOPIC, {}, sender='tn.rogue')
        clock['now'] += 31
        agent.vip.pubsub.deliver(TOPIC, {}, sender='tn.rogue')
    assert recorder.calls == []
    warnings = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    rogue = [w for w in warnings if "'tn.rogue'" in w]
    assert len(rogue) == 2, 'one line at the first drop, one when the minute has passed; the rest counted'
    assert '5 more from this sender dropped' in rogue[1]
    assert len([w for w in warnings if "'tn.other'" in w]) == 1, 'each sender has its own window'


# --- the key in the configuration ---------------------------------------------------------------------------------------

def configured_agent(**neighbor_overrides):
    contents = deepcopy(CONFIG)
    contents['neighbors'][0].update(neighbor_overrides)
    agent = TransactiveNodeAgent()
    agent.configure_main('config', 'NEW', contents)
    return agent, contents


def test_the_sender_key_is_read_by_the_agent_and_stripped_before_tent_sees_it():
    agent, contents = configured_agent(sender='tn.city@city')
    assert agent._neighbor_senders == {'CoR': 'tn.city@city'}
    assert 'sender' not in agent.configuration['neighbors'][0], 'TENT keeps the configuration without the agent key'
    assert agent.neighbors[0].config.model_dump().get('sender') is None
    assert contents['neighbors'][0]['sender'] == 'tn.city@city', "the caller's dict is untouched"


def test_a_signal_from_the_wrong_sender_never_reaches_the_neighbor(caplog):
    agent, _ = configured_agent(sender='tn.city@city')
    caplog.clear()  # the configuration's own log lines are not what this test is about
    with caplog.at_level(logging.WARNING):
        agent.vip.pubsub.deliver(TOPIC, {'not': 'a signal'}, sender='tn.rogue')
    assert not any(r.name.startswith('tent.') for r in caplog.records), 'the neighbor saw nothing'
    assert any('Dropped a transactive signal' in r.getMessage() for r in caplog.records)
    caplog.clear()
    with caplog.at_level(logging.WARNING):
        agent.vip.pubsub.deliver(TOPIC, {'not': 'a signal'}, sender='tn.city')
    assert any(r.name.startswith('tent.') for r in caplog.records), 'the neighbor received (and rejected) the message'


def test_an_unbound_transactive_neighbor_is_warned_about_at_configuration(caplog):
    with caplog.at_level(logging.WARNING, logger='transactive_node.agent'):
        agent, _ = configured_agent()
    assert agent._neighbor_senders == {}
    unbound = [r for r in caplog.records if 'not bound to a sender' in r.getMessage()]
    assert len(unbound) == 1 and "'CoR'" in unbound[0].getMessage()


def test_a_non_transactive_neighbor_without_a_binding_is_not_warned_about(caplog):
    with caplog.at_level(logging.WARNING, logger='transactive_node.agent'):
        configured_agent(transactive=False)
    assert not any('not bound to a sender' in r.getMessage() for r in caplog.records)


def test_an_invalid_sender_value_is_refused_before_the_node_is_built():
    agent = TransactiveNodeAgent()
    contents = deepcopy(CONFIG)
    contents['neighbors'][0]['sender'] = ''
    with pytest.raises(ValueError, match='sender must be a non-empty string'):
        agent.configure_main('config', 'NEW', contents)
    assert not agent._configured and agent.vip.pubsub.subscriptions == []


def test_a_changed_binding_is_taken_in_place_on_update():
    agent, contents = configured_agent(sender='tn.city@city')
    changed = deepcopy(contents)
    changed['neighbors'][0]['sender'] = 'tn.city2@city'
    first_id = agent.id
    agent.configure_main('config', 'UPDATE', changed)
    assert agent.id == first_id and agent._neighbor_senders == {'CoR': 'tn.city2@city'}
    agent.vip.pubsub.deliver(TOPIC, {'x': 1}, sender='tn.city')  # the old identity no longer matches
    assert agent._sender_mismatches.get('tn.city', {}).get('count') == 0  # one warning line, nothing suppressed yet
