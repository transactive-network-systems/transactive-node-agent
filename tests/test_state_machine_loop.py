"""state_machine_loop: yields once per iteration even without markets and survives a failing iteration."""
import logging

import pytest

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
