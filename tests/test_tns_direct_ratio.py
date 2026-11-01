"""TNSDirectRatioActuationManager calls the actuator's set_point and revert_point with the device path and point name
split out of the actuation key, as the EnergyPlus agent's RPC interface requires."""
import logging

import pytest

from transactive_node.local_asset.actuation_manager.tns_direct_ratio import TNSDirectRatioActuationManager

LOGGER = 'transactive_node.local_asset.actuation_manager.tns_direct_ratio'
KEY = 'Campus/SMALL_OFFICE_DR/HP1/ZoneCoolingTemperatureSetPoint'


class Occupancy:
    def check_schedule(self, when):
        return True


class Parent:
    name = 'ModelFrame'


def make_manager(node, **kwargs):
    return TNSDirectRatioActuationManager(actuator_identity='platform.actuator', transactive_node=node, parent=Parent(),
                                          occupancy_manager=Occupancy(), **kwargs)


def test_requires_an_actuator_identity(node):
    with pytest.raises(ValueError):
        TNSDirectRatioActuationManager(actuator_identity='', transactive_node=node, parent=Parent(),
                                       occupancy_manager=Occupancy())


def test_actuation_key_is_split_into_device_path_and_point_name():
    assert TNSDirectRatioActuationManager.split_point(KEY) == ('Campus/SMALL_OFFICE_DR/HP1',
                                                               'ZoneCoolingTemperatureSetPoint')
    assert TNSDirectRatioActuationManager.split_point('/A/B/') == ('A', 'B')
    with pytest.raises(ValueError):
        TNSDirectRatioActuationManager.split_point('JustAPoint')


def test_set_point_is_called_positionally_with_path_point_and_value(node):
    node.vip.rpc.reply = 24.5
    manager = make_manager(node, rpc_timeout=7)
    manager.direct_actuate(KEY, 24.5)
    assert node.vip.rpc.calls == [('platform.actuator', 'set_point',
                                   ('Campus/SMALL_OFFICE_DR/HP1', 'ZoneCoolingTemperatureSetPoint', 24.5), {})]


def test_revert_point_is_called_with_path_and_point_and_a_failure_reply_is_an_error(node, caplog):
    node.vip.rpc.reply = 'SUCCESS'
    manager = make_manager(node)
    with caplog.at_level(logging.ERROR, logger=LOGGER):
        manager.direct_release(KEY)
    assert node.vip.rpc.calls == [('platform.actuator', 'revert_point',
                                   ('Campus/SMALL_OFFICE_DR/HP1', 'ZoneCoolingTemperatureSetPoint'), {})]
    assert caplog.records == []
    node.vip.rpc.reply = 'FAILURE'
    with caplog.at_level(logging.ERROR, logger=LOGGER):
        manager.direct_release(KEY)
    assert len(caplog.records) == 1 and "reply 'FAILURE'" in caplog.records[0].getMessage()


def test_remote_failure_is_logged_at_error_with_the_actuators_reply(node, caplog):
    from volttron.platform.jsonrpc import RemoteError
    node.vip.rpc.error = RemoteError('Failed to find a known topic to set for parameters: path=Campus/X')
    manager = make_manager(node)
    with caplog.at_level(logging.ERROR, logger=LOGGER):
        manager.direct_actuate(KEY, 25.0)
        manager.direct_release(KEY)
    messages = [r.getMessage() for r in caplog.records if r.levelno == logging.ERROR]
    assert len(messages) == 2 and all('Failed to find a known topic' in m for m in messages)
    assert 'Failed to set' in messages[0] and 'Failed to revert' in messages[1]
