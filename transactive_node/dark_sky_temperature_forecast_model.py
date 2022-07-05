import gevent
import logging

from dateutil import parser

from tent.temperature_forecast_model import TemperatureForecastModel
from tent.utils.log import setup_logging

from volttron.platform.jsonrpc import RemoteError

setup_logging()
_log = logging.getLogger(__name__)


class DarkSkyTemperatureForecastModel(TemperatureForecastModel):
    def __init__(self,
                 remote: dict = None,
                 remote_platform: str = '',
                 weather_vip: str = '',
                 *args, **kwargs):
        super(DarkSkyTemperatureForecastModel, self).__init__(*args, **kwargs)

        remote = dict(remote) if remote else {}
        if remote and self.tn and self.tn():
            self.connection = self.tn().vip.auth.connect_remote_platform(address=(remote.get("address")),
                                                                         serverkey=(remote.get("server_key")))
        self.weather_vip = str(weather_vip) if weather_vip else "platform.weather_service"
        self.remote_platform = str(remote_platform)
        # there is no easy way to check if weather service is running on a remote platform
        if not (self.tn and self.tn() and self.weather_vip in self.tn().vip.peerlist.list().get())\
                and self.remote_platform is None:
            _log.warning("Weather service is not running!")

    def query_weather_data(self):
        """
        Use VOLTTRON DarkSky weather agent running on local or remote platform to get 24 hour forecast for weather data.
        """
        weather_results = self._rpc_handler()
        return self._parse_rpc_data(weather_results)

    def _rpc_handler(self):
        attempts = 0
        success = False
        result = []
        weather_results = None
        while not success and attempts < 10:
            try:
                result = self.connection.vip.rpc.call(self.weather_vip,
                                                      "get_hourly_forecast",
                                                      self.location,
                                                      external_platform=self.remote_platform).get(timeout=15)

                weather_results = result[0]["weather_results"]
                success = True
            except (gevent.Timeout, RemoteError) as ex:
                _log.warning("RPC call to {} failed for WEATHER forecast: {}".format(self.weather_vip, ex))
                attempts += 1
            except KeyError as ex:
                _log.debug("No WEATHER Results!: {} -- {}".format(result, ex))
                attempts += 1
        if attempts >= 10:
            _log.debug("10 Failed attempts to get WEATHER forecast via RPC!!!")
            return weather_results
        return weather_results

    def _parse_rpc_data(self, weather_results):
        try:
            weather_data = [[parser.parse(oat[0]).astimezone(self.local_tz), oat[1][self.oat_point_name]] for oat in
                            weather_results]
            weather_data = [[oat[0].replace(tzinfo=None), oat[1]] for oat in weather_data]
            _log.debug("Parsed WEATHER information: {}".format(weather_data))
        except KeyError:
            weather_data = []
            _log.debug("Measurement WEATHER Point Name is not correct")
        # How do we deal with never getting weather information?  Exit?
        except Exception as ex:
            weather_data = []
            _log.debug("Exception {} processing WEATHER data.".format(ex))
        return weather_data
