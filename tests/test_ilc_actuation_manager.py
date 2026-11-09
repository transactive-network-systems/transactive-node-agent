"""ILCActuationManager: requires a node and publishes targets through the node's publication queue."""
import logging
from datetime import datetime, timedelta, timezone

import pytest

from transactive_node.local_asset.actuation_manager.ilc import ILCActuationManager


class Occupancy:
    def check_schedule(self, when):
        return True


class Parent:
    name = 'ModelFrame'


def make_manager(node):
    return ILCActuationManager(ilc_target_topic='record/target_agent', transactive_node=node, parent=Parent(),
                               occupancy_manager=Occupancy())


def test_requires_a_node_and_a_topic(node):
    with pytest.raises(ValueError):
        ILCActuationManager(ilc_target_topic='record/target_agent', parent=Parent(), occupancy_manager=Occupancy())
    with pytest.raises(ValueError):
        ILCActuationManager(ilc_target_topic='', transactive_node=node, parent=Parent(), occupancy_manager=Occupancy())


def test_set_target_publishes_the_negated_target_with_interval_bounds(node, fixed_now):
    manager = make_manager(node)
    start = datetime(2026, 10, 31, 13, 0, tzinfo=timezone.utc)
    manager.set_target(12.5, start, start + timedelta(hours=1))
    topic, headers, message = node.published[0]
    assert node.vip.pubsub.published == [], 'nothing is published on the platform directly'
    assert topic == 'record/target_agent'
    assert headers == {'Timestamp': fixed_now.isoformat(), 'Datetime': fixed_now.isoformat()}
    assert message[0]['value'] == {'id': f'ModelFrame_{start}', 'target': -12.5, 'start': start.isoformat(),
                                   'end': (start + timedelta(hours=1)).isoformat()}
    assert message[1]['value'] == {'units': 'kW', 'tz': 'UTC'}


def test_release_target_publishes_a_null_target(node):
    manager = make_manager(node)
    start = datetime(2026, 10, 31, 13, 0, tzinfo=timezone.utc)
    manager.release_target(start, start + timedelta(hours=1))
    assert node.published[0][2][0]['value']['target'] is None


def test_the_target_topic_is_not_reconfigurable_and_unknown_keys_are_errors(node):
    from tent.config import ConfigurationError, ReconfigurationError
    manager = make_manager(node)
    with pytest.raises(ReconfigurationError):
        manager.reconfigure({'ilc_target_topic': 'record/other'})
    with pytest.raises(ConfigurationError, match="unknown configuration key 'ilc_topic'"):
        ILCActuationManager(ilc_topic='record/x', transactive_node=node, parent=Parent(), occupancy_manager=Occupancy())

