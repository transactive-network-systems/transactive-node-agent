"""ILCActuationManager: requires a node, publishes targets, logs publish failures with traceback."""
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
    topic, headers, message = node.vip.pubsub.published[0]
    assert topic == 'record/target_agent'
    assert headers == {'Timestamp': fixed_now.isoformat(), 'Datetime': fixed_now.isoformat()}
    assert message[0]['value'] == {'id': f'ModelFrame_{start}', 'target': -12.5, 'start': start.isoformat(),
                                   'end': (start + timedelta(hours=1)).isoformat()}
    assert message[1]['value'] == {'units': 'kW', 'tz': 'UTC'}


def test_release_target_publishes_a_null_target(node):
    manager = make_manager(node)
    start = datetime(2026, 10, 31, 13, 0, tzinfo=timezone.utc)
    manager.release_target(start, start + timedelta(hours=1))
    assert node.vip.pubsub.published[0][2][0]['value']['target'] is None


def test_publish_failure_is_logged_as_an_error_with_traceback(node, caplog):
    manager = make_manager(node)

    def failing_publish(**kwargs):
        raise RuntimeError('bus is down')
    node.vip.pubsub.publish = failing_publish
    start = datetime(2026, 10, 31, 13, 0, tzinfo=timezone.utc)
    with caplog.at_level(logging.ERROR, logger='transactive_node.local_asset.actuation_manager.ilc'):
        manager.set_target(1.0, start, start + timedelta(hours=1))
    errors = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert len(errors) == 1 and errors[0].exc_info is not None and 'record/target_agent' in errors[0].getMessage()


def test_the_target_topic_is_not_reconfigurable_and_unknown_keys_are_errors(node):
    from tent.config import ConfigurationError, ReconfigurationError
    manager = make_manager(node)
    with pytest.raises(ReconfigurationError):
        manager.reconfigure({'ilc_target_topic': 'record/other'})
    with pytest.raises(ConfigurationError, match="unknown configuration key 'ilc_topic'"):
        ILCActuationManager(ilc_topic='record/x', transactive_node=node, parent=Parent(), occupancy_manager=Occupancy())


def test_a_node_with_a_publication_queue_gets_the_target_queued_not_published_in_line(node, fixed_now):
    """24 direct publishes of one market's targets held the deployed ILC office's state machine for 20 s (T-74): a node
    that offers publish_to_topic (the agent) receives the target there and nothing goes to the bus synchronously."""
    queued = []
    node.publish_to_topic = lambda topic, headers, message: queued.append((topic, headers, message))
    manager = make_manager(node)
    start = datetime(2026, 10, 31, 13, 0, tzinfo=timezone.utc)
    manager.set_target(12.5, start, start + timedelta(hours=1))
    assert node.vip.pubsub.published == []
    topic, headers, message = queued[0]
    assert topic == 'record/target_agent'
    assert headers == {'Timestamp': fixed_now.isoformat(), 'Datetime': fixed_now.isoformat()}
    assert message[0]['value']['target'] == -12.5 and message[1]['value'] == {'units': 'kW', 'tz': 'UTC'}
