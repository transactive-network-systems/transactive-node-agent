"""configure_main: NEW builds the node; UPDATE tears the previous configuration down and rebuilds from a copy."""
from copy import deepcopy

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
    agent.configure_main('config', 'NEW', contents)
    first_id = agent.id
    first_subscriptions = list(agent.vip.pubsub.subscriptions)
    first_greenlet = agent.core.spawned[0]
    agent._stop_agent = True  # as onstop would leave it; a rebuilt node must run again

    agent.configure_main('config', 'UPDATE', contents)  # the config store hands back the same contents

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
