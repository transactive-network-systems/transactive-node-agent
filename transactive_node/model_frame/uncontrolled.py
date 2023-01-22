import datetime
import logging

from volttron.platform.agent import utils

_log = logging.getLogger(__name__)
utils.setup_logging()

class UncontrolledLoad(object):
    def __init__(self, config, **kwargs):
        self.actuation_topic = None
        self.load_schedule = config['uncontrolled_load_schedule']
        self.n_points = config.get("demand_curve_points", 2)

    def predict_flexibility(self, params=None):
        power = self.predict_power(params)
        return [power]*self.n_points

    def predict_power(self, params, set_point=None):
        interval_time: datetime = params.get('interval_time')
        index = interval_time.hour
        power = self.load_schedule[index]
        return -power

    @staticmethod
    def set_point_range(interval_start_time: datetime):
        return None, None

    @staticmethod
    def update_data(self):
        pass