import logging

from datetime import datetime

from tent.meter_point.push_meter_point import PushMeterPoint
from tent.utils.helpers import validate_bool

_log = logging.getLogger(__name__)


class TNSMeterPoint(PushMeterPoint):
    def __init__(self, topic, point_name, external_platform, hook=None, *args, **kwargs):
        self.topic = str(topic)
        self.point_name = str(point_name)
        self.external_platform = validate_bool(external_platform, 'external_platform')
        super().__init__(subscribe_key=self.topic, callback=self.on_topic, *args, **kwargs)

    def on_topic(self, headers, message):
        if self.tn and not self.tn.simulation:
            date_header = headers.get('Date')
            d_time = datetime.fromisoformat(date_header) if date_header is not None else None
        else:
            d_time = None
        datum = message[0].get(self.point_name)
        if datum:
            self.set_meter_value(datum, d_time)
        else:
            _log.warning(f'Received bad message. Unable to parse meter values: {message}')
