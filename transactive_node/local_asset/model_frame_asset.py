import importlib
import logging

from datetime import datetime
from typing import List

from transactive_node.model_frame import ModelFrame
from transactive_node.local_asset.occupancy_manager import OccupancyManager

from tent.containers.interval_value import IntervalValue
from tent.containers.time_interval import TimeInterval
from tent.containers.vertex import Vertex
from tent.enumerations.market_state import MarketState
from tent.enumerations.measurement_type import MeasurementType
from tent.local_asset import LocalAsset
from tent.market import Market
from tent.utils.helpers import get_class_from_spec
from tent.utils.log import setup_logging

from volttron.platform.messaging import headers as headers_mod

setup_logging()
_log = logging.getLogger(__name__)


class ModelFrameAsset(LocalAsset, ModelFrame):
    def __init__(self,
                 model_configs: dict = None,
                 temperature_forecast_name: str = '',
                 actuation_manager: dict = None,
                 occupancy_manager: dict = None,
                 *args, **kwargs):
        model_configs = model_configs if model_configs else {}
        ModelFrame.__init__(self, model_configs, **kwargs)
        LocalAsset.__init__(self, *args, **kwargs)

        self.temperature_forecast_name = temperature_forecast_name
        # self.actuation_method = actuation_method
        # self.actuator_identity = actuator_identity
        # self.ilc_target_topic = ilc_target_topic

        # Initialize Scheduler
        if occupancy_manager:  # TODO: Implement default behavior if occupancy_manager is not specified (make an always occupied base class).
            om_class = get_class_from_spec(occupancy_manager, 'OccupancyManager', 'transactive_node.local_asset')
            self.occupancy_manager: OccupancyManager = om_class(**occupancy_manager)

        if self.tn and self.tn():
            tn = self.tn()
            for device_topic in self.models:
                if 'Uncontrolled' in device_topic:
                    continue
                _log.info("Subscribing to " + device_topic)
                tn.vip.pubsub.subscribe(peer="pubsub", prefix=device_topic, callback=self.new_model_data)

            # Initialize Actuation Manager
            if actuation_manager:
                # TODO: The default module_prefix doesn't quite work with the default class.
                #  Need better detection logic in the helper function.
                am_class = get_class_from_spec(actuation_manager, 'ActuationManager', transactive_node=self.tn,
                                               default_module_prefix='transactive_node.local_asset.actuation_manager')
                self.actuation_manager = am_class(parent=self, occupancy_manager=self.occupancy_manager,
                                                  **actuation_manager)

    def new_model_data(self, peer, sender, bus, topic, header, message):
        """Ingest new data for models in ModelFrame."""
        now = datetime.fromisoformat(header[headers_mod.TIMESTAMP])
        data, meta = message
        if topic in self.models:
            self.models[topic].update_data(data, now)

    def _get_scheduled_power_from_model(self, time_interval: TimeInterval, outside_air_temperature) -> float:
        """Return a schedule power value for one interval from a model."""
        occupied = self.occupancy_manager.check_schedule(time_interval.start_time)
        params = {'interval_time': time_interval.start_time, 'OAT': outside_air_temperature, 'occupied': occupied}
        return self.model_power(params=params)

    def _get_power_flexibility_from_model(self, time_interval: TimeInterval, outside_air_temperature) -> List[float]:
        occupied = self.occupancy_manager.check_schedule(time_interval.start_time)
        params = {'interval_time': time_interval.start_time, 'OAT': outside_air_temperature, 'occupied': occupied}
        flexibility = self.model_flexibility(params=params)
        min_power = min(flexibility)
        max_power = max(flexibility)
        if min_power == max_power:
            min_power = min_power - 1e-10
        return [min_power, max_power]

    def _get_price_flexibility(self, time_interval: TimeInterval,   market: Market,
                               multiplier: float = 1.0) -> List[float]:
        # interval_price = market.marginalPrices.get_by_interval(time_interval)
        # effective_price = interval_price.value if interval_price else market.defaultPrice
        # min_price = 0.8 * effective_price
        # max_price = 1.2 * effective_price
        start_time = time_interval.start_time
        average_price, standard_deviation = market.price_model.get(start_time)
        min_price = average_price - (multiplier * standard_deviation)
        max_price = average_price + (multiplier * standard_deviation)
        return [min_price, max_price]

    def _create_vertices(self, power_flexibility: List[float], price_flexibility: List[float]) -> List[Vertex]:
        # Create the vertex that can represent this (lack of) flexibility
        # Vertex(marginal_price=, prod_cost=0.0, power=)
        lower_vertex = Vertex(price=price_flexibility[0], cost=0.0, quantity=power_flexibility[0])
        upper_vertex = Vertex(price=price_flexibility[1], cost=0.0, quantity=power_flexibility[1])
        return [lower_vertex, upper_vertex]

    def schedule_power(self, market):
        # Determine powers of an asset in active time intervals.
        # - Updates self.scheduledPowers - the schedule of power consumed

        # Get temperature forecast:
        temperature_forecast = [x for x in self.information_services if x.name == self.temperature_forecast_name][0]
        default_value = self.default_power

        for time_interval in market.time_intervals:
            # Get temperature for this interval:
            outside_air_temperature = temperature_forecast.predicted_values.get_by_interval(time_interval).value
            # Get new scheduled power value for this time interval:
            modeled_value = self._get_scheduled_power_from_model(time_interval, outside_air_temperature)
            value = modeled_value if modeled_value else default_value  # Use default if modeled values is not available.

            # Check whether a scheduled power already exists for the indexed time interval:
            self.scheduled_powers.update(time_interval, value)

        self.schedule_calculated = True

    def update_vertices(self, market):
        """Create vertices to represent the asset's flexibility"""
        # Gather and sort active time intervals:

        # Get temperature forecast
        temperature_forecast = [x for x in self.information_services if x.name == self.temperature_forecast_name][0]

        # Index through active time intervals.
        for time_interval in market.time_intervals:
            # Get temperature for this interval:
            outside_air_temperature = temperature_forecast.predicted_values.get_by_interval(time_interval).value

            # Get physical flexibility:
            power_flexibility = self._get_power_flexibility_from_model(time_interval, outside_air_temperature)

            # Get price flexibility:
            price_flexibility = self._get_price_flexibility(time_interval, market)

            vertices = self._create_vertices(power_flexibility, price_flexibility)
            self.activeVertices = [x for x in self.activeVertices if x.interval != time_interval]
            for vertex in vertices:
                self.activeVertices.append(
                    IntervalValue(self, time_interval, market, MeasurementType.ActiveVertex, vertex)
                )

        # Trim the list of active vertices so that it will not grow indefinitely.
        self.activeVertices = [x for x in self.activeVertices if x.market.market_state != MarketState.Expired]

    def actuate(self, mkt: Market):
        self.actuation_manager.actuate(mkt)
