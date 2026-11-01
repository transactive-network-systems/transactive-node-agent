"""TNSMeterPoint: a zero is a reading, non-mapping payloads and missing points are rejected without raising."""
import logging
from datetime import datetime, timezone

import pytest

from transactive_node.tns_meter_point import TNSMeterPoint


def make_meter(node):
    return TNSMeterPoint(subscribe_key='METERS', point_name='WholeBuildingPower',
                         transactive_node=node, name='Meter', measurement_type='AverageDemandkW',
                         measurement_unit='kWh')


def test_subscribes_through_the_node_under_its_key(node):
    meter = make_meter(node)
    assert node.subscriptions == {'METERS': meter.handle_subscription}


def test_zero_is_a_reading(node):
    meter = make_meter(node)
    when = datetime(2026, 10, 31, 12, 0, tzinfo=timezone.utc)
    meter.handle_subscription(when, 'METERS', {'WholeBuildingPower': 0.0, 'Other': 5})
    assert meter.current_measurement == 0.0 and meter.last_updated == when
    readings = [r for r in node.data_manager.records if r['table_name'] == 'meter_reading']
    assert len(readings) == 1 and readings[0]['data']['value'] == 0.0


def test_non_mapping_payload_is_rejected_without_raising(node, caplog):
    meter = make_meter(node)
    with caplog.at_level(logging.WARNING, logger='transactive_node.tns_meter_point'):
        meter.handle_subscription(datetime.now(timezone.utc), 'METERS', [{'WholeBuildingPower': 1.0}])
        meter.handle_subscription(datetime.now(timezone.utc), 'METERS', 'WholeBuildingPower=1')
        meter.handle_subscription(datetime.now(timezone.utc), 'METERS', None)
    assert len(meter.buffer) == 0
    assert sum('expected a mapping' in r.getMessage() for r in caplog.records) == 3


def test_missing_point_and_wrong_key_are_ignored(node, caplog):
    meter = make_meter(node)
    with caplog.at_level(logging.WARNING, logger='transactive_node.tns_meter_point'):
        meter.handle_subscription(datetime.now(timezone.utc), 'METERS', {'SomethingElse': 3.0})
        meter.handle_subscription(datetime.now(timezone.utc), 'HP1', {'WholeBuildingPower': 3.0})
    assert len(meter.buffer) == 0
    messages = [r.getMessage() for r in caplog.records]
    assert any('no value for point' in m for m in messages) and any('unexpected subscribe_key' in m for m in messages)


def test_simulation_stamps_readings_with_the_simulated_clock(simulated_node, fixed_now):
    meter = make_meter(simulated_node)
    meter.handle_subscription(datetime(2026, 1, 1, tzinfo=timezone.utc), 'METERS', {'WholeBuildingPower': 2.5})
    assert meter.last_updated == fixed_now and meter.current_measurement == 2.5


def test_unknown_keys_and_a_missing_point_name_are_configuration_errors(node):
    from tent.config import ConfigurationError
    with pytest.raises(ConfigurationError, match="TNSMeterPoint 'Meter': unknown configuration key 'external_platform'"):
        TNSMeterPoint(transactive_node=node, name='Meter', subscribe_key='METERS', point_name='P',
                      external_platform=False)
    with pytest.raises(ConfigurationError, match='point_name is required'):
        TNSMeterPoint(transactive_node=node, name='Meter', subscribe_key='METERS')
    assert node.subscriptions == {}, 'nothing was subscribed for a rejected configuration'
