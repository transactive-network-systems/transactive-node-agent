"""HOST_KEYS is the one statement of the keys the agent reads and removes before TENT sees a configuration: the agent
pops exactly those, nothing else, and the module is importable without the platform."""
import subprocess
import sys
from copy import deepcopy
from pathlib import Path

import pytest

from transactive_node import HOST_KEYS, strip_host_keys
from transactive_node.agent import TransactiveNodeAgent
from test_configure import CONFIG

SRC = str(Path(__file__).resolve().parent.parent / 'src')


def every_host_key_set():
    """CONFIG with every HOST_KEYS key present (top level and per neighbor), at non-default values."""
    contents = deepcopy(CONFIG)
    contents.update({'db_topic': 'tns', 'subscribe_all_platforms': True,
                     'topics': {'METERS': 'devices/Campus/B/METERS'}, 'slow_iteration_warning': 7})
    for entry in contents['neighbors']:
        entry['sender'] = f'tn.{entry["name"].lower()}@{entry["name"].lower()}'
    assert set(contents) >= HOST_KEYS[''] and all(HOST_KEYS['neighbors'] <= set(n) for n in contents['neighbors'])
    return contents


def test_host_keys_names_the_top_level_and_neighbor_keys():
    assert HOST_KEYS[''] == {'db_topic', 'subscribe_all_platforms', 'topics', 'slow_iteration_warning'}
    assert HOST_KEYS['neighbors'] == {'sender'}
    assert set(HOST_KEYS) == {'', 'neighbors'}
    with pytest.raises(TypeError):
        HOST_KEYS['local_assets'] = frozenset()  # read-only


def test_strip_host_keys_removes_exactly_the_host_keys_and_reports_their_values():
    contents = every_host_key_set()
    contents['unknown_to_everyone'] = 1
    tent_part, host = strip_host_keys(contents)
    assert not (set(tent_part) & HOST_KEYS['']) and 'unknown_to_everyone' in tent_part, 'only host keys are removed'
    assert all(not (set(n) & HOST_KEYS['neighbors']) for n in tent_part['neighbors'])
    assert host[''] == {'db_topic': 'tns', 'subscribe_all_platforms': True, 'topics': {'METERS': 'devices/Campus/B/METERS'},
                        'slow_iteration_warning': 7}
    assert host['neighbors'] == {'CoR': {'sender': 'tn.cor@cor'}}
    assert 'sender' in contents['neighbors'][0] and 'db_topic' in contents, 'the given configuration is untouched'
    assert strip_host_keys({'name': 'bare'}) == ({'name': 'bare'}, {'': {}, 'neighbors': {}})


def test_the_agent_pops_every_host_key_and_nothing_else_before_tent_sees_the_configuration():
    contents = every_host_key_set()
    agent = TransactiveNodeAgent()
    agent.configure_main('config', 'NEW', contents)
    reached_tent = agent.configuration
    assert not (set(reached_tent) & HOST_KEYS['']), 'every top-level host key was removed'
    assert all(not (set(n) & HOST_KEYS['neighbors']) for n in reached_tent['neighbors'])
    tent_part, _ = strip_host_keys(contents)
    tent_part.pop('simulation_anchor_time', None)
    reached = dict(reached_tent)
    reached.pop('simulation_anchor_time', None)
    assert reached == tent_part, 'nothing but the host keys (and the environment anchor) differs from what was given'
    # ... and the agent read each of them.
    assert (agent.db_topic, agent.subscribe_all_platforms) == ('tns', True)
    assert agent.configured_topics == {'METERS': 'devices/Campus/B/METERS'} and agent.slow_iteration_warning == 7.0
    assert agent._neighbor_senders == {'CoR': 'tn.cor@cor'}


def test_a_changed_rebuilding_host_key_rebuilds_and_the_others_apply_in_place():
    agent = TransactiveNodeAgent()
    contents = every_host_key_set()
    agent.configure_main('config', 'NEW', contents)
    data_manager = agent.data_manager
    changed = deepcopy(contents)
    changed['slow_iteration_warning'] = 9
    changed['neighbors'][0]['sender'] = 'tn.other@cor'
    agent.configure_main('config', 'UPDATE', changed)
    assert agent.data_manager is data_manager, 'taken in place'
    assert agent.slow_iteration_warning == 9.0 and agent._neighbor_senders == {'CoR': 'tn.other@cor'}
    changed['subscribe_all_platforms'] = False
    agent.configure_main('config', 'UPDATE', changed)
    assert agent.data_manager is not data_manager, 'subscriptions derive from it: rebuilt'
    assert all(not everywhere for prefix, _, everywhere in agent.vip.pubsub.subscriptions if not prefix.endswith('/status'))


def test_host_keys_import_without_the_platform():
    code = ("import sys; sys.modules['volttron'] = None; sys.modules['gevent'] = None; sys.modules['tent'] = None\n"
            "from transactive_node import HOST_KEYS, strip_host_keys\n"
            "print(sorted(HOST_KEYS['']))")
    result = subprocess.run([sys.executable, '-c', code], capture_output=True, text=True,
                            env={'PYTHONPATH': SRC, 'PATH': ''})
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "['db_topic', 'slow_iteration_warning', 'subscribe_all_platforms', 'topics']"
