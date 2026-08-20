import logging

from datetime import datetime

from tent.meter_point.push_meter_point import PushMeterPoint
from tent.utils.helpers import validate_bool

_log = logging.getLogger(__name__)


class TNSMeterPoint(PushMeterPoint):
    def __init__(self, subscribe_key: str, point_name: str, external_platform: bool, *args, **kwargs):
        # subscribe_key should be a string which is mapped to a topic in the TNS agent configuration "topics" dict.
        self.point_name = str(point_name)
        self.external_platform = validate_bool(external_platform, 'external_platform')
        super().__init__(subscribe_key=subscribe_key, *args, **kwargs)

    def handle_subscription(self, time_received: datetime, subscribe_key: str, message: dict):
        if subscribe_key != self.subscribe_key:
            _log.warning(f'Received unexpected subscribe_key "{subscribe_key}" in MeterPoint expecting "{self.subscribe_key}"')
        if self.tn and not self.tn.simulation:
            d_time = time_received if time_received is not None else None
        else:
            d_time = None
        datum = message.get(self.point_name)
        if datum:
            self.set_meter_value(datum, d_time)
        else:
            _log.warning(f'Received bad message. Unable to parse meter values: {message}')
