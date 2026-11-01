"""WeatherDotGovTemperatureForecastModel: one RPC per update, aware times in the node's zone, borrowing for the
current hour, fallback to the TMY file when the service fails, validation of the configuration."""
import logging
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import gevent
import pytest

from tent.containers.time_interval import TimeInterval
from transactive_node.weather_dot_gov_temperature_forecast_model import WeatherDotGovTemperatureForecastModel

LOGGER = 'transactive_node.weather_dot_gov_temperature_forecast_model'
LOCATION = {'lat': 46.2804, 'long': -119.2752}


class Market:
    def __init__(self, name, first_start, duration, count):
        self.name = name
        self.data_manager = None
        self.time_intervals = [TimeInterval(self, duration, first_start + duration * i) for i in range(count)]


def reply_for(hours, first=datetime(2026, 10, 31, 13, 0, tzinfo=timezone.utc), unit='F', point='air_temperature',
              base_temperature=50.0, zone=ZoneInfo('America/Los_Angeles')):
    """A weather agent answer: hourly records from the next full hour, stamped in the Pacific zone as NOAA does."""
    records = []
    for i in range(hours):
        when = (first + timedelta(hours=i)).astimezone(zone)
        records.append([when.isoformat(), {point: base_temperature + i, 'temperatureUnit': unit, 'number': i + 1}])
    return [dict(LOCATION, generation_time=first.isoformat(), weather_results=records)]


def make_model(node, **overrides):
    kwargs = dict(weather_vip='platform.weather', location=LOCATION, transactive_node=node, name='TemperatureForecast')
    kwargs.update(overrides)
    return WeatherDotGovTemperatureForecastModel(**kwargs)


def values_for(model, market):
    return [model.predicted_values.get_by_interval(ti).value for ti in market.time_intervals]


def test_requires_an_identity_a_location_mapping_and_a_point_name(node):
    with pytest.raises(ValueError):
        make_model(node, weather_vip='')
    with pytest.raises(ValueError):
        make_model(node, location='')
    with pytest.raises(ValueError):
        make_model(node, location={})
    with pytest.raises(ValueError):
        make_model(node, temperature_point_name='')


def test_defaults_target_the_weather_agents_standard_temperature_point(node):
    model = make_model(node)
    assert model.location == [LOCATION] and model.oat_point_name == 'air_temperature'
    assert model.weather_file_path is None, 'service mode: no file replaces the service'


def test_one_rpc_covers_the_market_and_every_interval_gets_its_hour(node, fixed_now):
    # Market of four hours starting now (12:00Z). The service answers from 13:00Z, so 12:00Z borrows 13:00Z.
    market = Market('RT', fixed_now, timedelta(hours=1), 4)
    node.vip.rpc.reply = reply_for(6)
    model = make_model(node)

    model.update_information(market)

    peer, method, args, kwargs = node.vip.rpc.calls[0]
    assert (peer, method) == ('platform.weather', 'get_hourly_forecast')
    assert args == ([LOCATION], 5), 'ceil(4 h to the end of the market) + 1 for the hour the service skips'
    assert kwargs == {}
    assert len(node.vip.rpc.calls) == 1
    assert values_for(model, market) == [50.0, 50.0, 51.0, 52.0]
    assert model.last_modified == fixed_now
    times = [ti.start_time for ti, _ in model.predicted_values.all]
    assert all(t.tzinfo is not None for t in times)




def test_node_zone_is_applied_and_sub_hour_intervals_share_their_hour(node_factory, fixed_now):
    node = node_factory(tz='America/New_York')
    start = fixed_now + timedelta(hours=1)  # 13:00Z: eight 15-minute intervals through 15:00Z
    market = Market('RT15', start, timedelta(minutes=15), 8)
    node.vip.rpc.reply = reply_for(4)
    model = make_model(node)
    assert model.update_information(market) is None
    assert values_for(model, market) == [50.0] * 4 + [51.0] * 4


def test_celsius_records_are_converted_to_fahrenheit(node, fixed_now):
    market = Market('DA', fixed_now + timedelta(hours=1), timedelta(hours=1), 2)
    node.vip.rpc.reply = reply_for(3, unit='C', base_temperature=10.0)
    model = make_model(node)
    model.update_information(market)
    assert values_for(model, market) == [50.0, 51.8]


def test_external_platform_is_passed_to_the_rpc(node, fixed_now):
    market = Market('DA', fixed_now + timedelta(hours=1), timedelta(hours=1), 1)
    node.vip.rpc.reply = reply_for(2)
    make_model(node, external_platform='central').update_information(market)
    assert node.vip.rpc.calls[0][3] == {'external_platform': 'central'}


def test_hours_requested_are_bounded_by_the_service_horizon(node, fixed_now):
    far = Market('long', fixed_now, timedelta(hours=1), 400)
    assert make_model(node).forecast_hours_for(far) == 156
    past = Market('old', fixed_now - timedelta(days=2), timedelta(hours=1), 2)
    assert make_model(node).forecast_hours_for(past) == 1


def test_gap_beyond_max_gap_time_assigns_nothing_and_logs(node, fixed_now, caplog):
    market = Market('DA', fixed_now + timedelta(hours=1), timedelta(hours=1), 8)
    node.vip.rpc.reply = reply_for(2)  # 13:00Z and 14:00Z only; 15:00Z..20:00Z are up to six hours away
    model = make_model(node, max_gap_time=3600)
    with caplog.at_level(logging.WARNING, logger=LOGGER):
        model.update_information(market)
    assert len(model.predicted_values) == 0
    assert any('does not cover intervals' in r.getMessage() for r in caplog.records)
    assert any('no fallback file' in r.getMessage() and r.levelno == logging.ERROR for r in caplog.records)


@pytest.fixture
def tmy_file(tmp_path, fixed_now):
    path = tmp_path / 'tmy.csv'
    rows = ['Timestamp,Value'] + [f'{(fixed_now + timedelta(hours=i)).isoformat()},{70 + i}' for i in range(6)]
    path.write_text('\n'.join(rows) + '\n')
    return str(path)


def test_rpc_failure_is_logged_once_and_the_fallback_file_is_used(node, fixed_now, tmy_file, caplog):
    market = Market('RT', fixed_now, timedelta(hours=1), 3)
    import gevent
    node.vip.rpc.error = gevent.Timeout()
    model = make_model(node, tmy_fallback_path=tmy_file)
    with caplog.at_level(logging.WARNING, logger=LOGGER):
        model.update_information(market)
    assert len(node.vip.rpc.calls) == 1, 'no retry loop'
    assert values_for(model, market) == [70.0, 71.0, 72.0]
    errors = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert len(errors) == 1 and 'get_hourly_forecast' in errors[0].getMessage()
    assert any('fallback file' in r.getMessage() for r in caplog.records)


def test_weather_error_and_missing_point_are_reported(node, fixed_now, caplog):
    market = Market('RT', fixed_now + timedelta(hours=1), timedelta(hours=1), 1)
    model = make_model(node)
    node.vip.rpc.reply = [dict(LOCATION, weather_error='Invalid location')]
    with caplog.at_level(logging.ERROR, logger=LOGGER):
        model.update_information(market)
    assert any('Invalid location' in r.getMessage() for r in caplog.records)
    caplog.clear()
    node.vip.rpc.reply = reply_for(2, point='temperature')
    with caplog.at_level(logging.ERROR, logger=LOGGER):
        model.update_information(market)
    assert any("'air_temperature' not in forecast record" in r.getMessage() for r in caplog.records)
    assert len(model.predicted_values) == 0


def test_naive_forecast_time_is_rejected(node, fixed_now, caplog):
    market = Market('RT', fixed_now + timedelta(hours=1), timedelta(hours=1), 1)
    node.vip.rpc.reply = [dict(LOCATION, weather_results=[['2026-10-31T13:00:00', {'air_temperature': 50}]])]
    with caplog.at_level(logging.ERROR, logger=LOGGER):
        make_model(node).update_information(market)
    assert any('timezone-aware' in r.getMessage() for r in caplog.records)


def test_archive_record_carries_the_location_and_point(node):
    model = make_model(node)
    record = model.get_record()
    assert record['location'] == [LOCATION] and record['temperature_point_name'] == 'air_temperature'
    assert record['information_type'].name == 'Temperature'
