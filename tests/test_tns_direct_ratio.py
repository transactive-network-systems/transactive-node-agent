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


def test_actuator_identity_and_timeout_are_reconfigurable_in_place(node):
    from tent.config import ConfigurationError, ReconfigurationError
    manager = make_manager(node)
    manager.reconfigure({'actuator_identity': 'energyplus.actuator', 'rpc_timeout': 3.0})
    node.vip.rpc.reply = 1.0
    manager.direct_actuate(KEY, 1.0)
    assert node.vip.rpc.calls[0][0] == 'energyplus.actuator' and node.vip.rpc.calls[0][2][2] == 1.0
    with pytest.raises(ReconfigurationError):
        manager.reconfigure({'control_interval': 5})
    with pytest.raises(ConfigurationError, match="unknown configuration key 'actuator'"):
        make_manager(node, actuator='platform.actuator')


# --- all of a market's points at once -------------------------------------------------------------------------------------

import time as _time

from tent.local_asset.actuation_manager.direct_ratio import DirectRatioActuationManager


class LatentResult:
    """A reply that becomes available `latency` seconds after the call was issued; get() waits for it (or raises)."""
    def __init__(self, ready_at, value, error=None):
        self.ready_at, self.value, self.error = ready_at, value, error

    def get(self, timeout=None):
        _time.sleep(max(0.0, self.ready_at - _time.monotonic()))
        if self.error is not None:
            raise self.error
        return self.value


class LatentRpc:
    def __init__(self, latency, failing_point=None, reply='SUCCESS'):
        self.latency, self.failing_point, self.reply = latency, failing_point, reply
        self.calls = []       # (method, path, point, value or None, issued at)
        self.gets = []

    def call(self, peer, method, path, point_name, *args, **kwargs):
        from volttron.platform.jsonrpc import RemoteError
        self.calls.append((method, path, point_name, args[0] if args else None, _time.monotonic()))
        error = RemoteError('Failed to set value of ' + point_name) if point_name == self.failing_point else None
        return LatentResult(_time.monotonic() + self.latency, args[0] if args else self.reply, error)


KEYS = [f'Campus/SMALL_OFFICE_DR/HP{i}/ZoneCoolingTemperatureSetPoint' for i in range(1, 11)] + \
       [f'Campus/SMALL_OFFICE_DR/LIGHTING/G{i}/DimmingLevelOutput' for i in range(1, 11)]


def drive_base_through(manager, monkeypatch, method, per_point):
    """Stand in for DirectRatioActuationManager.actuate/release: the base class's loop over the models, calling the
    per-point hook for each of the twenty keys, exactly as TENT does."""
    def base(self, mkt):
        for index, key in enumerate(KEYS):
            per_point(self, key, index)
    monkeypatch.setattr(DirectRatioActuationManager, method, base)


def test_twenty_points_are_set_together_not_one_round_trip_at_a_time(node, monkeypatch):
    node.vip.rpc = LatentRpc(latency=0.2)
    manager = make_manager(node, rpc_timeout=5)
    drive_base_through(manager, monkeypatch, 'actuate', lambda m, key, i: m.direct_actuate(key, 20.0 + i))
    started = _time.monotonic()
    manager.actuate(mkt=None)
    elapsed = _time.monotonic() - started
    assert elapsed < 1.0, f'twenty 0.2 s round trips took {elapsed:.2f} s; they must overlap, not add up to 4 s'
    assert [(m, f'{p}/{n}', v) for m, p, n, v, _ in node.vip.rpc.calls] == \
        [('set_point', key, 20.0 + i) for i, key in enumerate(KEYS)], 'every point, with its value, in order'
    issued = [t for *_, t in node.vip.rpc.calls]
    assert max(issued) - min(issued) < 0.1, 'all calls were issued before any reply was waited for'
    assert manager._pending_sets is None


def test_a_failing_point_is_logged_and_the_others_are_still_set(node, monkeypatch, caplog):
    node.vip.rpc = LatentRpc(latency=0.01, failing_point='ZoneCoolingTemperatureSetPoint')
    manager = make_manager(node)
    drive_base_through(manager, monkeypatch, 'actuate', lambda m, key, i: m.direct_actuate(key, 21.0))
    with caplog.at_level(logging.ERROR, logger=LOGGER):
        manager.actuate(mkt=None)
    errors = [r.getMessage() for r in caplog.records if r.levelno == logging.ERROR]
    assert len(errors) == 10 and all('Failed to set' in e and 'ZoneCoolingTemperatureSetPoint' in e for e in errors)
    assert len(node.vip.rpc.calls) == 20, 'the lighting points were still set'


def test_releases_are_sent_together_and_a_failure_reply_is_reported(node, monkeypatch, caplog):
    node.vip.rpc = LatentRpc(latency=0.05, reply='SUCCESS')
    manager = make_manager(node)
    drive_base_through(manager, monkeypatch, 'release', lambda m, key, i: m.direct_release(key))
    started = _time.monotonic()
    with caplog.at_level(logging.ERROR, logger=LOGGER):
        manager.release(mkt=None)
    assert _time.monotonic() - started < 0.5 and len(node.vip.rpc.calls) == 20
    assert [m for m, *_ in node.vip.rpc.calls] == ['revert_point'] * 20 and caplog.records == []
    node.vip.rpc = LatentRpc(latency=0.0, reply='FAILURE')
    with caplog.at_level(logging.ERROR, logger=LOGGER):
        manager.release(mkt=None)
    assert len([r for r in caplog.records if "reply 'FAILURE'" in r.getMessage()]) == 20


def test_the_overall_timeout_bounds_the_wait_and_logs_each_unanswered_point(node, monkeypatch, caplog):
    import gevent

    class NeverResult:
        def get(self, timeout=None):
            raise gevent.Timeout()

    class SilentRpc(LatentRpc):
        def call(self, peer, method, path, point_name, *args, **kwargs):
            self.calls.append((method, path, point_name, args[0] if args else None, _time.monotonic()))
            return NeverResult()
    node.vip.rpc = SilentRpc(latency=0)
    manager = make_manager(node, rpc_timeout=0.5)
    drive_base_through(manager, monkeypatch, 'actuate', lambda m, key, i: m.direct_actuate(key, 1.0))
    with caplog.at_level(logging.ERROR, logger=LOGGER):
        manager.actuate(mkt=None)
    assert len([r for r in caplog.records if 'Timeout' in r.getMessage()]) == 20


def test_direct_calls_outside_a_market_still_work_one_at_a_time(node):
    node.vip.rpc = LatentRpc(latency=0.0)
    manager = make_manager(node)
    manager.direct_actuate(KEYS[0], 22.0)
    manager.direct_release(KEYS[1])
    assert [(m, v) for m, _, _, v, _ in node.vip.rpc.calls] == [('set_point', 22.0), ('revert_point', None)]
