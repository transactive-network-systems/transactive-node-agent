"""configure_main: NEW builds the node; UPDATE tears the previous configuration down and rebuilds from a copy, keeping
the run identity when the simulation settings are unchanged; the simulation anchor may come from the environment."""
import logging
from copy import deepcopy

import pytest

from transactive_node.agent import TransactiveNodeAgent

CONFIG = {
    'name': 'Campus',
    'tz': 'UTC',
    'simulation': False,
    'db_topic': 'tns',
    'subscribe_all_platforms': True,
    'topics': {'METERS': 'devices/Campus/B/METERS'},
    'data_manager': {'class_name': 'VolttronDataManager', 'module_name': 'transactive_node.data_manager',
                     'historian_vip': 'tns.historian'},
    'meter_points': [{'class_name': 'TNSMeterPoint', 'module_name': 'transactive_node.tns_meter_point',
                      'name': 'Meter', 'point_name': 'WholeBuildingPower', 'external_platform': False,
                      'subscribe_key': 'METERS', 'measurement_type': 'AverageDemandkW', 'measurement_unit': 'kWh'}],
    'neighbors': [{'class_name': 'Neighbor', 'module_name': 'tent.neighbor', 'name': 'CoR', 'transactive': True,
                   'up_or_down': 'upstream'}],
}


def prefixes(pubsub):
    return sorted(prefix for prefix, _, _ in pubsub.subscriptions)


def test_new_configuration_builds_the_node_and_starts_the_loop():
    agent = TransactiveNodeAgent()
    agent.configure_main('config', 'NEW', deepcopy(CONFIG))

    assert agent.name == 'Campus' and [m.name for m in agent.meter_points] == ['Meter']
    assert [n.name for n in agent.neighbors] == ['CoR']
    assert prefixes(agent.vip.pubsub) == ['devices/Campus/B/METERS', 'tns/CoR/Campus']
    assert all(all_platforms for _, _, all_platforms in agent.vip.pubsub.subscriptions)
    assert agent.topics['archive'] == 'tns/archive/Campus/records'
    assert agent.topics['archive_definitions'] == 'tns/archive/Campus/definitions'
    assert agent.topics['archive_status'] == 'tns/archive/Campus/status'
    assert agent.vip.rpc.calls[0][:2] == ('tns.historian', 'create_tables')
    archive_topics = [topic for topic, _, _ in agent.vip.pubsub.published]
    assert archive_topics and set(archive_topics) == {'tns/archive/Campus/records'}
    (greenlet,) = agent.core.spawned
    assert greenlet.func == agent.state_machine_loop and not greenlet.dead


def test_update_tears_down_the_previous_configuration_and_rebuilds():
    agent = TransactiveNodeAgent()
    contents = deepcopy(CONFIG)
    contents['simulation'] = True
    contents['simulation_start_time'] = '2017-07-03 00:00:00'
    contents['simulation_one_hour_in_seconds'] = 100
    agent.configure_main('config', 'NEW', contents)
    first_id = agent.id
    first_subscriptions = list(agent.vip.pubsub.subscriptions)
    first_greenlet = agent.core.spawned[0]
    agent._stop_agent = True  # as onstop would leave it; a rebuilt node must run again

    contents['simulation_one_hour_in_seconds'] = 200  # a changed simulation block: the rebuild is a new run
    agent.configure_main('config', 'UPDATE', contents)

    assert first_greenlet.killed and agent._state_machine_greenlet is agent.core.spawned[1]
    assert not agent._stop_agent
    assert sorted(p for p, _, _ in agent.vip.pubsub.unsubscribed) == sorted(p for p, _, _ in first_subscriptions)
    assert all(callback in {w for _, w, _ in first_subscriptions} for _, callback, _ in agent.vip.pubsub.unsubscribed)
    assert prefixes(agent.vip.pubsub) == ['devices/Campus/B/METERS', 'tns/CoR/Campus'], 'rebuilt once, not stacked'
    assert not any(w in {w0 for _, w0, _ in first_subscriptions} for _, w, _ in agent.vip.pubsub.subscriptions)
    assert [m.name for m in agent.meter_points] == ['Meter'] and [n.name for n in agent.neighbors] == ['CoR']
    assert agent.id != first_id, 'the rebuilt node is a new run'
    assert agent.vip.rpc.calls[1][:2] == ('tns.historian', 'create_tables')


def test_configuration_contents_are_not_consumed():
    agent = TransactiveNodeAgent()
    contents = deepcopy(CONFIG)
    agent.configure_main('config', 'NEW', contents)
    assert contents == CONFIG, 'class_name and module_name must survive for the next UPDATE'


def test_update_with_a_changed_topic_subscribes_only_to_the_new_one():
    agent = TransactiveNodeAgent()
    agent.configure_main('config', 'NEW', deepcopy(CONFIG))
    changed = deepcopy(CONFIG)
    changed['topics'] = {'METERS': 'devices/Campus/C/METERS'}
    changed['neighbors'] = []
    agent.configure_main('config', 'UPDATE', changed)
    assert prefixes(agent.vip.pubsub) == ['devices/Campus/C/METERS'] and agent.neighbors == []


def test_pubsub_registration_subscribes_to_the_status_topic_on_all_platforms_and_is_torn_down_on_update():
    agent = TransactiveNodeAgent()
    contents = deepcopy(CONFIG)
    contents['subscribe_all_platforms'] = False
    contents['data_manager'] = {'class_name': 'VolttronDataManager', 'module_name': 'transactive_node.data_manager',
                                'historian_vip': 'tns.historian', 'registration': 'pubsub', 'rpc_total_timeout': 0}
    pubsub = agent.vip.pubsub

    def answer_definitions(peer, topic, headers=None, message=None, **kwargs):
        pubsub.published.append((topic, headers, message))
        if topic == 'tns/archive/Campus/definitions':
            status = {'request_id': message['request_id'], 'node': 'Campus', 'status': 'success',
                      'registered_tables': [d['name'] for d in message['definitions']], 'rejected_tables': {},
                      'message': ''}
            pubsub.deliver('tns/archive/Campus/status', status, headers=headers)
    pubsub.publish = answer_definitions

    agent.configure_main('config', 'NEW', contents)
    status_subscriptions = [(p, a) for p, _, a in pubsub.subscriptions if p == 'tns/archive/Campus/status']
    assert status_subscriptions == [('tns/archive/Campus/status', True)], 'subscribed everywhere, regardless of the node setting'
    assert all(not a for p, _, a in pubsub.subscriptions if p != 'tns/archive/Campus/status')
    assert agent.data_manager.archive_ready and agent.vip.rpc.calls == []
    assert [t for t, _, _ in pubsub.published if t.endswith('/records')], 'the queued rows were flushed'

    agent.configure_main('config', 'UPDATE', contents)
    assert 'tns/archive/Campus/status' in {p for p, _, _ in pubsub.unsubscribed}
    assert len([p for p, _, _ in pubsub.subscriptions if p == 'tns/archive/Campus/status']) == 1


SIMULATED = dict(CONFIG, simulation=True, simulation_start_time='2017-07-03 00:00:00',
                 simulation_one_hour_in_seconds=100, simulation_anchor_time='2026-10-31T12:00:00+00:00')


def test_update_with_unchanged_simulation_settings_continues_the_run(caplog):
    agent = TransactiveNodeAgent()
    agent.configure_main('config', 'NEW', deepcopy(SIMULATED))
    first_id, first_created = agent.id, agent.real_time_created
    changed = deepcopy(SIMULATED)
    changed['neighbors'] = []  # anything but the simulation block
    with caplog.at_level(logging.INFO, logger='transactive_node.agent'):
        agent.configure_main('config', 'UPDATE', changed)
    assert (agent.id, agent.real_time_created) == (first_id, first_created)
    assert agent.neighbors == [] and agent.simulation_anchor_time.isoformat() == '2026-10-31T12:00:00+00:00'
    assert any('same run' in r.getMessage() for r in caplog.records)


@pytest.mark.parametrize('key, value', [('simulation_one_hour_in_seconds', 50),
                                        ('simulation_start_time', '2017-07-04 00:00:00'),
                                        ('simulation_anchor_time', '2026-10-31T13:00:00+00:00'),
                                        ('simulation', False)])
def test_update_with_changed_simulation_settings_is_a_new_run(key, value, caplog):
    agent = TransactiveNodeAgent()
    agent.configure_main('config', 'NEW', deepcopy(SIMULATED))
    first_id = agent.id
    changed = deepcopy(SIMULATED)
    changed[key] = value
    with caplog.at_level(logging.INFO, logger='transactive_node.agent'):
        agent.configure_main('config', 'UPDATE', changed)
    assert agent.id != first_id
    assert any('new run' in r.getMessage() for r in caplog.records)


def test_a_new_configuration_is_always_a_new_run():
    agent = TransactiveNodeAgent()
    agent.configure_main('config', 'NEW', deepcopy(SIMULATED))
    first_id = agent.id
    agent.configure_main('config', 'NEW', deepcopy(SIMULATED))
    assert agent.id != first_id


def test_anchor_comes_from_the_environment_when_the_configuration_has_none(monkeypatch, caplog):
    monkeypatch.setenv('TENT_SIMULATION_ANCHOR_TIME', '2026-10-31T09:30:00+00:00')
    contents = deepcopy(SIMULATED)
    del contents['simulation_anchor_time']
    agent = TransactiveNodeAgent()
    with caplog.at_level(logging.INFO, logger='transactive_node.agent'):
        agent.configure_main('config', 'NEW', contents)
    assert agent.simulation_anchor_time.isoformat() == '2026-10-31T09:30:00+00:00'
    assert 'simulation_anchor_time' not in contents, 'the caller\'s dict is untouched'
    assert any('from the environment' in r.getMessage() for r in caplog.records)


def test_configured_anchor_wins_over_the_environment(monkeypatch):
    monkeypatch.setenv('TENT_SIMULATION_ANCHOR_TIME', '2026-10-31T09:30:00+00:00')
    agent = TransactiveNodeAgent()
    agent.configure_main('config', 'NEW', deepcopy(SIMULATED))
    assert agent.simulation_anchor_time.isoformat() == '2026-10-31T12:00:00+00:00'


def test_without_configuration_or_environment_the_anchor_is_the_node_creation_time(monkeypatch):
    monkeypatch.delenv('TENT_SIMULATION_ANCHOR_TIME', raising=False)
    contents = deepcopy(SIMULATED)
    del contents['simulation_anchor_time']
    agent = TransactiveNodeAgent()
    agent.configure_main('config', 'NEW', contents)
    assert agent.simulation_anchor_time == agent.real_time_created


def test_environment_anchor_keeps_the_run_across_an_update(monkeypatch):
    monkeypatch.setenv('TENT_SIMULATION_ANCHOR_TIME', '2026-10-31T09:30:00+00:00')
    contents = deepcopy(SIMULATED)
    del contents['simulation_anchor_time']
    agent = TransactiveNodeAgent()
    agent.configure_main('config', 'NEW', deepcopy(contents))
    first_id = agent.id
    agent.configure_main('config', 'UPDATE', deepcopy(contents))
    assert agent.id == first_id
