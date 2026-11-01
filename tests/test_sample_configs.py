"""Every sample configuration builds a node through configure_main; the component sample constructs its class."""
import json
from copy import deepcopy
from pathlib import Path

import pytest

from tent.information_service_model import InformationServiceModel
from tent.utils.helpers import get_class_from_spec, spec_kwargs
from transactive_node.agent import TransactiveNodeAgent

SAMPLES = Path(__file__).resolve().parent.parent / 'sample_config'
NODE_SAMPLES = sorted(p for p in SAMPLES.glob('*.json') if 'class_name' not in json.loads(p.read_text()))
COMPONENT_SAMPLES = sorted(p for p in SAMPLES.glob('*.json') if 'class_name' in json.loads(p.read_text()))


def test_the_sample_directory_holds_the_four_deployed_nodes():
    assert [p.stem for p in NODE_SAMPLES] == ['campus', 'city', 'small_office_dr', 'small_office_ilc']


@pytest.mark.parametrize('path', NODE_SAMPLES, ids=lambda p: p.stem)
def test_node_sample_configures_an_agent(path):
    config = json.loads(path.read_text())
    # The data manager is replaced so no sqlite file is written and the historian is the fake RPC peer.
    config['data_manager'] = {'class_name': 'VolttronDataManager', 'module_name': 'transactive_node.data_manager',
                              'historian_vip': 'tns.historian'}
    agent = TransactiveNodeAgent()
    agent.configure_main('config', 'NEW', deepcopy(config))
    assert agent.name == config['name']
    assert len(agent.market_series) == len(config['market_series'])
    assert [n.name for n in agent.neighbors] == [n['name'] for n in config['neighbors']]
    assert [a.name for a in agent.local_assets] == [a['name'] for a in config['local_assets']]
    subscribed = {prefix for prefix, _, _ in agent.vip.pubsub.subscriptions}
    for neighbor in config['neighbors']:
        assert f"{config['db_topic']}/{neighbor['name']}/{config['name']}" in subscribed
    for meter in config.get('meter_points', []):
        assert config['topics'][meter['subscribe_key']] in subscribed


@pytest.mark.parametrize('path', COMPONENT_SAMPLES, ids=lambda p: p.stem)
def test_component_sample_constructs(path, node):
    spec = json.loads(path.read_text())
    component_class = get_class_from_spec(spec, InformationServiceModel)
    component = component_class(transactive_node=node, **spec_kwargs(spec))
    assert component.name == 'TemperatureForecast'
