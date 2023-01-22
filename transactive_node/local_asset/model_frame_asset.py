import importlib
import logging

from typing import List

from transactive_node.model_frame import ModelFrame

from tent.containers.interval_value import IntervalValue
from tent.containers.time_interval import TimeInterval
from tent.containers.vertex import Vertex
from tent.enumerations.market_state import MarketState
from tent.enumerations.measurement_type import MeasurementType
from tent.local_asset import LocalAsset
from tent.market import Market
from tent.utils.helpers import find_obj_by_ti
from tent.utils.log import setup_logging

from volttron.platform.agent.utils import parse_timestamp_string
from volttron.platform.messaging import headers as headers_mod

setup_logging()
_log = logging.getLogger(__name__)


class ModelFrameAsset(LocalAsset, ModelFrame):
    def __init__(self,
                 model_configs: dict = None,
                 temperature_forecast_name: str = '',
                 actuation_manager: dict = None,
                 *args, **kwargs):
        model_configs = model_configs if model_configs else {}
        ModelFrame.__init__(self, model_configs, **kwargs)
        LocalAsset.__init__(self, *args, **kwargs)

        self.temperature_forecast_name = temperature_forecast_name
        # self.actuation_method = actuation_method
        # self.actuator_identity = actuator_identity
        # self.ilc_target_topic = ilc_target_topic

        if self.tn and self.tn():
            tn = self.tn()
            for device_topic in self.models:
                if 'Uncontrolled' in device_topic:
                    continue
                _log.info("Subscribing to " + device_topic)
                tn.vip.pubsub.subscribe(peer="pubsub", prefix=device_topic, callback=self.new_model_data)

            # Initialize Actuation Manager
            if actuation_manager:
                am_class = actuation_manager.pop('class_name', 'ActuationManager')
                am_module = actuation_manager.pop('module_name', 'transactive_node.local_asset.actuation_manager')
                module = importlib.import_module(am_module)
                cls = getattr(module, am_class)
                self.actuation_manager = cls(parent=self, transactive_node=tn, **actuation_manager)

    def new_model_data(self, peer, sender, bus, topic, header, message):
        """Ingest new data for models in ModelFrame."""
        now = parse_timestamp_string(header[headers_mod.TIMESTAMP])
        data, meta = message
        if topic in self.models:
            self.models[topic].update_data(data, now)

    def _get_scheduled_power_from_model(self, time_interval: TimeInterval, outside_air_temperature) -> float:
        """Return a schedule power value for one interval from a model."""
        params = {'interval_time': time_interval.startTime, 'OAT': outside_air_temperature}
        return self.model_power(params=params)

    def _get_power_flexibility_from_model(self, time_interval: TimeInterval, outside_air_temperature) -> List[float]:
        params = {'interval_time': time_interval.startTime, 'OAT': outside_air_temperature}
        flexibility = self.model_flexibility(params=params)
        min_power = min(flexibility)
        max_power = max(flexibility)
        if min_power == max_power:
            min_power = min_power - 1e-10
        return [min_power, max_power]

    def _get_price_flexibility(self, time_interval: TimeInterval, market: Market,
                               multiplier: float = 1.0) -> List[float]:
        # interval_price = find_obj_by_ti(market.marginalPrices, time_interval)
        # effective_price = interval_price.value if interval_price else market.defaultPrice
        # min_price = 0.8 * effective_price
        # max_price = 1.2 * effective_price
        start_time = time_interval.startTime
        average_price, standard_deviation = market.priceModel.get(start_time)
        min_price = average_price - (multiplier * standard_deviation)
        max_price = average_price + (multiplier * standard_deviation)
        return [min_price, max_price]

    def _create_vertices(self, power_flexibility: List[float], price_flexibility: List[float]) -> List[Vertex]:
        # Create the vertex that can represent this (lack of) flexibility
        # Vertex(marginal_price=, prod_cost=0.0, power=)
        lower_vertex = Vertex(marginal_price=price_flexibility[0], prod_cost=0.0, power=power_flexibility[0])
        upper_vertex = Vertex(marginal_price=price_flexibility[1], prod_cost=0.0, power=power_flexibility[1])
        return [lower_vertex, upper_vertex]

    def schedule_power(self, market):
        # Determine powers of an asset in active time intervals.

        # PRESUMPTIONS:
        # - Active time intervals exist and have been updated
        # - Marginal prices exist and have been updated.
        #   NOTE: Marginal prices are used only for elastic assets. This base class is provided for the simplest asset
        #         having constant power. It MUST be extended to represent dynamic and elastic asset behaviors.
        #   NOTE: (1911DJH) This requirement is loosened upon recognizing that auction markets have no forward prices
        #         available at the time an asset is called to schedule its power, i.e., to prepare its bid. This can be
        #         resolved by letting an asset model use its markets' price forecast models for forward intervals in
        #         which locational marginal prices have not yet been discovered.
        # OUTPUTS:
        # - Updates self.scheduledPowers - the schedule of power consumed

        # Gather and sort the active time intervals:
        time_intervals = market.timeIntervals
        time_intervals.sort(key=lambda x: x.startTime)

        # Get temperature forecast:
        temperature_forecast = [x for x in self.informationServices if x.name == self.temperature_forecast_name][0]
        temperatures = [iv for iv in temperature_forecast.predictedValues
                        if iv.measurementType == MeasurementType.Temperature]

        default_value = self.defaultPower

        for time_interval in time_intervals:
            # Get temperature for this interval:
            outside_air_temperature = find_obj_by_ti(temperatures, time_interval)
            # Get new scheduled power value for this time interval:
            modeled_value = self._get_scheduled_power_from_model(time_interval, outside_air_temperature)
            value = modeled_value if modeled_value else default_value  # Use default if modeled values is not available.

            # Check whether a scheduled power already exists for the indexed time interval:
            iv = find_obj_by_ti(self.scheduledPowers, time_interval)

            if iv is None:  # A scheduled power does not exist for the indexed time interval.
                # Create an interval value with the value and append it to scheduled powers:
                iv = IntervalValue(self, time_interval, market, MeasurementType.ScheduledPower, value)
                self.scheduledPowers.append(iv)
            else:
                # Reassign the value of the existing interval value:
                iv.value = value  # [avg.kW]

        # Remove expired intervals to prevent the list of scheduled powers from growing indefinitely:
        self.scheduledPowers = [x for x in self.scheduledPowers if x.market.marketState != MarketState.Expired]

        self.scheduleCalculated = True

    def update_vertices(self, market):
        """Create vertices to represent the asset's flexibility"""
        # Gather and sort active time intervals:
        time_intervals = market.timeIntervals
        time_intervals.sort(key=lambda x: x.startTime)

        # Get temperature forecast
        temperature_forecast = [x for x in self.informationServices if x.name == self.temperature_forecast_name][0]
        temperatures = [iv for iv in temperature_forecast.predictedValues
                        if iv.measurementType == MeasurementType.Temperature]

        # Index through active time intervals.
        for time_interval in time_intervals:
            # Get temperature for this interval:
            outside_air_temperature = find_obj_by_ti(temperatures, time_interval)

            # Get physical flexibility:
            power_flexibility = self._get_power_flexibility_from_model(time_interval, outside_air_temperature)

            # Get price flexibility:
            price_flexibility = self._get_price_flexibility(time_interval, market)

            vertices = self._create_vertices(power_flexibility, price_flexibility)
            self.activeVertices = [x for x in self.activeVertices if x.timeInterval != time_interval]
            for vertex in vertices:
                self.activeVertices.append(
                    IntervalValue(self, time_interval, market, MeasurementType.ActiveVertex, vertex)
                )

        # Trim the list of active vertices so that it will not grow indefinitely.
        self.activeVertices = [x for x in self.activeVertices if x.market.marketState != MarketState.Expired]

    def actuate(self, mkt: Market):
        self.actuation_manager.actuate(mkt)
        # # _log.info(f'Actuating via method: {self.actuation_method}')
        # scheduled_powers_for_mkt = [sp for sp in self.scheduledPowers if sp.market is mkt]
        # if self.actuation_method == 'ILC':
        #     for sp in scheduled_powers_for_mkt:
        #         # _log.debug(f'SETTING {sp.value} TARGET FOR {sp.timeInterval.startTime}')
        #         start_time = sp.timeInterval.startTime
        #         end_time = start_time + sp.timeInterval.duration
        #         sp_id = f'{self.name}_{start_time}'
        #         self._set_ilc_target(target_id=sp_id, target=sp.value, start=start_time, end=end_time)
        # elif self.actuation_method == 'DirectRatio':
        #     start_time = mkt.marketClearingTime + mkt.deliveryLeadTime
        #     price = [p.value for p in mkt.marginalPrices if p.timeInterval.startTime == start_time][0]
        #     vertex_prices = [av.value.marginalPrice for av in self.activeVertices
        #                      if av.timeInterval.startTime == start_time]
        #     min_price = min(vertex_prices)
        #     max_price = max(vertex_prices)
        #     price = min(max(price, min_price), max_price)  # clamp price within bid range.
        #     cleared_price_ratio = (price - min_price) / (max_price - min_price)
        #     for model in self.models.values():
        #         if model.actuation_topic:
        #             min_set_point, max_set_point = model.set_point_range(start_time)
        #             # For cooling mode:
        #             new_set_point = (cleared_price_ratio * (max_set_point - min_set_point)) + min_set_point
        #             # TODO: For heating mode instead do commented code:
        #             #new_set_point = max_set_point - (cleared_price_ratio * (
        #             #            max_set_point - min_set_point))
        #             self._direct_actuate(model.actuation_topic, new_set_point)
        # else:
        #     _log.warning(f'{self.name} received unknown actuation method: {self.actuation_method}')
        #     pass

    # def _direct_actuate(self, actuation_topic, actuation_set_point):
    #     if self.tn and self.tn():
    #         tn = self.tn()
    #         tn.vip.rpc.call(self.actuator_identity, 'set_point', requester_id=self.name, topic=actuation_topic,
    #                         value=actuation_set_point)
    #         # _log.debug(f'Actuation command of: {actuation_set_point} sent to {actuation_topic}')
    #
    # def _set_ilc_target(self, target_id: str, target: float, start: datetime, end: datetime):
    #     tn = self.tn()
    #     headers = {
    #         'Timestamp': format_timestamp(Timer.now()),
    #         'Datetime': format_timestamp(Timer.now())
    #     }
    #     target = [
    #         {
    #             "value": {
    #                 "id": target_id,
    #                 "target": -target,
    #                 "start": format_timestamp(start),
    #                 "end": format_timestamp(end)
    #             }
    #         },
    #         {
    #             "value": {
    #                 "units": "kW",
    #                 "tz": str(tn.tz)
    #             }
    #         }
    #     ]
    #     try:
    #         tn.vip.pubsub.publish(peer='pubsub', topic=self.ilc_target_topic,
    #                           headers=headers, message=target)
    #     except Exception as e:
    #         _log.debug(f'Error publishing to target: {e}')
