"""state_machine_loop: yields once per iteration even without markets, survives a failing iteration, hands control to
the hub at the inner yield points so messages are handled during a long iteration, and warns about slow iterations."""
import logging

import pytest

import transactive_node.agent as agent_module

from transactive_node.agent import TransactiveNodeAgent


@pytest.fixture
def agent(monkeypatch):
    agent = TransactiveNodeAgent()
    agent.sleeps = 0

    def counting_sleep():
        agent.sleeps += 1
    monkeypatch.setattr(agent, '_sleep', counting_sleep)
    return agent


def test_loop_without_markets_sleeps_once_per_iteration(agent, monkeypatch):
    iterations = []
    original = agent._state_machine_iteration

    def iteration():
        original()  # no market series configured: nothing to do, must still yield
        iterations.append(1)
        if len(iterations) == 3:
            agent._stop_agent = True
    monkeypatch.setattr(agent, '_state_machine_iteration', iteration)
    agent.state_machine_loop()
    assert len(iterations) == 3 and agent.sleeps == 3


def test_loop_logs_a_failing_iteration_with_traceback_and_keeps_going(agent, monkeypatch, caplog):
    calls = []

    def iteration():
        calls.append(1)
        if len(calls) == 1:
            raise RuntimeError('events blew up')
        if len(calls) == 3:
            agent._stop_agent = True
    monkeypatch.setattr(agent, '_state_machine_iteration', iteration)
    with caplog.at_level(logging.ERROR, logger='transactive_node.agent'):
        agent.state_machine_loop()
    assert len(calls) == 3, 'the loop continued after the failure'
    errors = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert len(errors) == 1 and errors[0].exc_info is not None and 'events blew up' in str(errors[0].exc_info[1])
    assert agent.sleeps == 3


def test_loop_does_not_run_when_already_stopped(agent, monkeypatch):
    monkeypatch.setattr(agent, '_state_machine_iteration', lambda: pytest.fail('iteration must not run'))
    agent._stop_agent = True
    agent.state_machine_loop()
    assert agent.sleeps == 0


class Hub:
    """Stands in for the gevent hub: work queued here (a pubsub delivery) runs when the agent yields."""
    def __init__(self):
        self.pending = []
        self.sleeps = []

    def sleep(self, seconds=0):
        self.sleeps.append(seconds)
        while self.pending:
            self.pending.pop(0)()


class Series(list):
    def prune_expired(self):
        pass


class LongMarket:
    """A market whose events take a while and yield to the node at inner points, as TENT's entities do."""
    def __init__(self, agent, name, yields=3, work=lambda: None):
        self.agent, self.name, self.yields, self.work = agent, name, yields, work
        self.seen_during_events = None

    def events(self):
        for _ in range(self.yields):
            self.work()
            self.agent._sleep()
        self.seen_during_events = list(self.agent.received)


@pytest.fixture
def hub_agent(monkeypatch):
    hub = Hub()
    monkeypatch.setattr(agent_module.gevent, 'sleep', hub.sleep)
    agent = TransactiveNodeAgent()
    agent.loop_sleep_duration = 0.25
    agent.topics = {'SIG': 'tns/CoR/Campus'}
    agent.received = []
    agent.register_subscription('SIG', lambda t, key, message: agent.received.append(message))
    return agent, hub


def test_inner_yields_are_free_and_the_loop_pauses_once_per_iteration(hub_agent):
    agent, hub = hub_agent
    agent.market_series = [Series([LongMarket(agent, 'DA', yields=4), LongMarket(agent, 'RT', yields=2)])]
    agent._state_machine_iteration()
    assert hub.sleeps == [0] * (4 + 1 + 2 + 1), 'every yield inside the iteration is gevent.sleep(0)'
    agent._sleep()  # what the loop does after the iteration
    assert hub.sleeps[-1] == 0.25 and not agent._iterating


def test_a_message_delivered_during_a_long_iteration_is_handled_before_the_iteration_ends(hub_agent):
    agent, hub = hub_agent
    market = LongMarket(agent, 'DA', yields=3)
    agent.market_series = [Series([market])]
    hub.pending.append(lambda: agent.vip.pubsub.deliver('tns/CoR/Campus', {'price': 1}, sender='tn.city'))
    agent._state_machine_iteration()
    assert market.seen_during_events == [{'price': 1}], 'handled at the first inner yield, not after the iteration'


def test_without_inner_yields_the_message_waits_for_the_iteration_to_end(hub_agent):
    agent, hub = hub_agent
    market = LongMarket(agent, 'DA', yields=0)
    agent.market_series = [Series([market])]
    hub.pending.append(lambda: agent.vip.pubsub.deliver('tns/CoR/Campus', {'price': 1}, sender='tn.city'))
    agent._state_machine_iteration()
    assert market.seen_during_events == [], 'the only yield is after the market; the message arrives afterwards'
    assert agent.received == [{'price': 1}]


def test_a_slow_iteration_is_warned_about_naming_the_longest_market(hub_agent, monkeypatch, caplog):
    agent, hub = hub_agent
    clock = {'now': 100.0}
    monkeypatch.setattr(agent_module.time, 'monotonic', lambda: clock['now'])

    def takes(seconds):
        return lambda: clock.__setitem__('now', clock['now'] + seconds)
    agent.slow_iteration_warning = 5.0
    agent.market_series = [Series([LongMarket(agent, 'DA', yields=1, work=takes(2.0)),
                                   LongMarket(agent, 'RT', yields=2, work=takes(2.0))])]
    with caplog.at_level(logging.WARNING, logger='transactive_node.agent'):
        agent._state_machine_iteration()
    warnings = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1 and "'RT'" in warnings[0] and 'took 6.0 s' in warnings[0] and 'at 4.0 s' in warnings[0]
    caplog.clear()
    agent.slow_iteration_warning = 10.0
    with caplog.at_level(logging.WARNING, logger='transactive_node.agent'):
        agent._state_machine_iteration()
    assert caplog.records == []


def test_a_failing_iteration_leaves_the_yield_mode_cleared(hub_agent, monkeypatch):
    agent, hub = hub_agent

    def boom():
        raise RuntimeError('events blew up')
    market = LongMarket(agent, 'DA', yields=1, work=boom)
    agent.market_series = [Series([market])]
    calls = []

    def once():
        calls.append(1)
        agent._stop_agent = True
        return agent_module.TransactiveNodeAgent._state_machine_iteration(agent)
    monkeypatch.setattr(agent, '_state_machine_iteration', once)
    agent.state_machine_loop()
    assert not agent._iterating and hub.sleeps[-1] == 0.25, 'the loop\'s pause is still a real one'
