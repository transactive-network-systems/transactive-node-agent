"""
Copyright (c) 2022, Battelle Memorial Institute
All rights reserved.
Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:
1. Redistributions of source code must retain the above copyright notice, this
   list of conditions and the following disclaimer.
2. Redistributions in binary form must reproduce the above copyright notice,
   this list of conditions and the following disclaimer in the documentation
   and/or other materials provided with the distribution.
THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
The views and conclusions contained in the software and documentation are those
of the authors and should not be interpreted as representing official policies,
either expressed or implied, of the FreeBSD Project.
This material was prepared as an account of work sponsored by an agency of the
United States Government. Neither the United States Government nor the United
States Department of Energy, nor Battelle, nor any of their employees, nor any
jurisdiction or organization that has cooperated in th.e development of these
materials, makes any warranty, express or implied, or assumes any legal
liability or responsibility for the accuracy, completeness, or usefulness or
any information, apparatus, product, software, or process disclosed, or
represents that its use would not infringe privately owned rights.
Reference herein to any specific commercial product, process, or service by
trade name, trademark, manufacturer, or otherwise does not necessarily
constitute or imply its endorsement, recommendation, or favoring by the
United States Government or any agency thereof, or Battelle Memorial Institute.
The views and opinions of authors expressed herein do not necessarily state or
reflect those of the United States Government or any agency thereof.

PACIFIC NORTHWEST NATIONAL LABORATORY
operated by BATTELLE for the UNITED STATES DEPARTMENT OF ENERGY
under Contract DE-AC05-76RL01830
"""
import gevent
import logging

from collections.abc import Mapping, Sequence
from datetime import datetime, timedelta
from math import ceil
from zoneinfo import ZoneInfo

from tent.information_service_model.temperature_forecast_model import TemperatureForecastModel
from tent.utils.helpers import parse_datetime, require_aware
from tent.utils.timer import Timer

from importlib.metadata import distribution, PackageNotFoundError
try:
    distribution('volttron-core')
    from volttron.client.vip.agent import errors
    from volttron.utils.jsonrpc import RemoteError
except PackageNotFoundError:
    from volttron.platform.jsonrpc import RemoteError
    from volttron.platform.vip.agent import errors

_log = logging.getLogger(__name__)


class WeatherDotGovTemperatureForecastModel(TemperatureForecastModel):
    """Hourly outdoor air temperature forecast from the VOLTTRON weather.gov agent (the NOAA service), by RPC.

    Each time an asset asks for an update for a market, one `get_hourly_forecast` call is made for enough hours to
    cover the market's intervals and every interval is given the forecast of the hour it starts in. The weather agent
    returns hours from the next full hour on, so an interval in the current hour borrows the nearest forecast hour;
    `max_gap_time` bounds how far an interval may borrow. When the service cannot be reached or the forecast does not
    cover the market, the base class's TMY fallback file (`tmy_fallback_path`) is used if one is configured. Nothing
    retries or blocks beyond the single RPC timeout: the market loop must not stall on the weather.

    Forecast times are aware throughout and are converted to the node's zone. Values are degrees Fahrenheit; a record
    whose `temperatureUnit` is Celsius is converted.

    Configuration: `weather_vip` (the weather agent's VIP identity, default `platform.weather`), `location` (one weather.gov location
    dictionary, required: {"lat": .., "long": ..}, {"station": "KPSC"} or {"wfo": .., "x": .., "y": ..}),
    `external_platform` (platform name when the weather agent runs elsewhere in the federation), `rpc_timeout`
    (seconds), `temperature_point_name` (default `air_temperature`, the weather agent's standard name for NOAA's
    `temperature`), `tmy_fallback_path` and `max_gap_time` as in the base class.
    """

    MAX_FORECAST_HOURS = 156  # weather.gov's hourly forecast horizon
    RPC_METHOD = 'get_hourly_forecast'

    def __init__(self,
                 location: Mapping,
                 weather_vip: str = 'platform.weather',
                 external_platform: str = '',
                 rpc_timeout: float = 15.0,
                 temperature_point_name: str = 'air_temperature',
                 *args, **kwargs):
        if not weather_vip:
            raise ValueError('weather_vip must name the weather agent\'s VIP identity (default platform.weather).')
        if not isinstance(location, Mapping) or not location:
            raise ValueError('WeatherDotGovTemperatureForecastModel requires location, a weather.gov location '
                             f'dictionary such as {{"lat": 46.23, "long": -119.22}}; got {location!r}.')
        if not temperature_point_name:
            raise ValueError('temperature_point_name must name the temperature point in the forecast records.')
        # weather_file_path stays None: the base class takes a file path to mean "forecast from the file, never from a
        # service". The fallback file, if any, is tmy_fallback_path.
        super(WeatherDotGovTemperatureForecastModel, self).__init__(location=dict(location),
                                                                    temperature_point_name=temperature_point_name,
                                                                    weather_file_path=None, *args, **kwargs)
        self.weather_vip: str = str(weather_vip)
        self.external_platform: str = str(external_platform) if external_platform else ''
        self.rpc_timeout: float = float(rpc_timeout)

    def update_information(self, market):
        forecast = self.query_weather_data(self.forecast_hours_for(market))
        if forecast and self.assign_forecast(market, forecast):
            self.last_modified = Timer.now()
            return
        if self.weather_file_path:
            _log.warning(f'{self.name}: no usable forecast from {self.weather_vip!r} for market {market.name}; '
                         f'using the fallback file {self.weather_file_path}.')
            self.get_forecast_file(market)
            return
        _log.error(f'{self.name}: no forecast for market {market.name} and no fallback file configured; '
                   f'predicted values are unchanged.')

    def forecast_hours_for(self, market) -> int:
        """Hours to request so the forecast, which starts at the next full hour, reaches the end of the market's last
        interval. At least one, at most the service's horizon."""
        end = max(ti.start_time + ti.duration for ti in market.time_intervals)
        hours = ceil((end - Timer.now()) / timedelta(hours=1)) + 1
        return min(max(hours, 1), self.MAX_FORECAST_HOURS)

    def query_weather_data(self, hours: int) -> list[tuple[datetime, float]]:
        """One forecast RPC to the weather agent. Returns (hour start in the node's zone, temperature in F) pairs sorted
        by time, or an empty list after logging what went wrong. Never raises and never retries."""
        kwargs = {'external_platform': self.external_platform} if self.external_platform else {}
        try:
            reply = self.tn.vip.rpc.call(self.weather_vip, self.RPC_METHOD, self.location, hours,
                                         **kwargs).get(timeout=self.rpc_timeout)
        except (gevent.Timeout, RemoteError, errors.VIPError) as e:
            _log.error(f'{self.name}: {self.RPC_METHOD} on {self.weather_vip!r} failed: {type(e).__name__}: {e}')
            return []
        try:
            return self.parse_forecast(reply)
        except (KeyError, TypeError, ValueError) as e:
            _log.error(f'{self.name}: unusable reply from {self.weather_vip!r}: {e}')
            return []

    def parse_forecast(self, reply) -> list[tuple[datetime, float]]:
        """The weather agent answers with one dictionary per requested location: `weather_results` as
        [[time, {point: value, ...}], ...], or `weather_error`, with optional `weather_warnings`."""
        if not isinstance(reply, Sequence) or len(reply) != 1 or not isinstance(reply[0], Mapping):
            raise ValueError(f'expected a list with one result for the one location, got {reply!r}')
        result = reply[0]
        if 'weather_error' in result:
            raise ValueError(f'weather_error for location {self.location[0]}: {result["weather_error"]}')
        for warning in result.get('weather_warnings', []):
            _log.warning(f'{self.name}: weather agent warning: {warning}')
        records = result.get('weather_results')
        if not records:
            raise ValueError(f'no weather_results in {result!r}')
        zone = ZoneInfo(self.tn.tz)
        forecast = []
        for forecast_time, points in records:
            when = require_aware(parse_datetime(forecast_time), 'forecast time').astimezone(zone)
            if self.oat_point_name not in points:
                raise KeyError(f'point {self.oat_point_name!r} not in forecast record; points are {sorted(points)}')
            value = float(points[self.oat_point_name])
            if points.get('temperatureUnit') == 'C':
                value = value * 9.0 / 5.0 + 32.0
            forecast.append((when.replace(minute=0, second=0, microsecond=0), value))
        return sorted(forecast)

    def assign_forecast(self, market, forecast: list[tuple[datetime, float]]) -> bool:
        """Give every interval of the market the forecast of the hour it starts in, borrowing the nearest hour within
        max_gap_time when that hour is absent. All or nothing: False, and predicted values untouched, if any interval
        has no forecast hour within reach."""
        by_hour = dict(forecast)
        assigned = []
        unassigned = []
        for interval in market.time_intervals:
            hour = interval.start_time.replace(minute=0, second=0, microsecond=0)
            if hour in by_hour:
                assigned.append((interval, by_hour[hour]))
                continue
            nearest = min(by_hour, key=lambda h: abs(h - hour))
            if abs(nearest - hour) <= self.max_gap_time:
                assigned.append((interval, by_hour[nearest]))
            else:
                unassigned.append(interval.start_time.isoformat())
        if unassigned:
            _log.warning(f'{self.name}: forecast from {self.weather_vip!r} ({min(by_hour).isoformat()} to '
                         f'{max(by_hour).isoformat()}) does not cover intervals starting {unassigned} of market '
                         f'{market.name}; nothing assigned.')
            return False
        for interval, value in assigned:
            self.predicted_values.update(interval, value)
        return True
