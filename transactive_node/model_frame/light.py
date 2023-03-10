import logging

from datetime import datetime
from volttron.platform.agent import utils
import numpy as np

_log = logging.getLogger(__name__)
utils.setup_logging()


class Lighting(object):
    def __init__(self, config, **kwargs):
        self.rated_power = config["rated_power"]
        self.max_set_point_offset = config.get('max_set_point_offset', 0.1)
        self.n_points = config.get("demand_curve_points", 2)
        self.actuation_topic = config.get('actuation_topic', None)
        try:
            self.lighting_schedule = config["default_lighting_schedule"]
        except KeyError:
            _log.warning("No no default lighting schedule!")
            self.lighting_schedule = [1.0]*24

    def predict_flexibility(self, params=None):
        interval_time: datetime = params.get('interval_time')
        min_set_point, max_set_point = self.set_point_range(interval_time)
        csp_flex = np.linspace(min_set_point, max_set_point, num=self.n_points)
        return [self.predict_power(params, csp) for csp in csp_flex]

    def predict_power(self, params=None, set_point=None):
        interval_time: datetime = params.get('interval_time')
        index = interval_time.hour
        # TODO: should this consider current set point, if it is the current interval?
        if not set_point:
            power = self.lighting_schedule[index]*self.rated_power
        else:
            power = set_point * self.rated_power
        return -power

    # TODO: Make nominal setpoint max and double default offset max.
    def set_point_range(self, interval_start_time: datetime):
        index = interval_start_time.hour
        set_point = self.lighting_schedule[index]
        return set_point - self.max_set_point_offset, set_point + self.max_set_point_offset

    def update_data(self, data, now):
        pass