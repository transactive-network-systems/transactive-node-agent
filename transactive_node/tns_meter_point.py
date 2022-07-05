from volttron.platform.agent import utils

from tent.push_meter_point import PushMeterPoint
from tent.helpers import *

setup_logging()
_log = logging.getLogger(__name__)


class TNSMeterPoint(PushMeterPoint):
    def __init__(self, topic, point_name, external_platform, hook=None, *args, **kwargs):
        self.hook = str(hook)
        self.topic = str(topic)
        self.point_name = str(point_name)
        self.external_platform = validate_bool(external_platform, 'external_platform')
        PushMeterPoint.__init__(self, *args, **kwargs)

    # Implements method from PushMeterPoint.
    def set_up_subscription(self):
        if self.topic and self.tn and self.tn():
            all_platforms = {'all_platforms': True} if self.external_platform else {}
            self.tn().vip.pubsub.subscribe('pubsub', self.topic, self.on_topic, **all_platforms)

    def on_topic(self, peer, sender, bus, topic, headers, message):
        if self.tn and self.tn() and not self.tn().simulation:
            date_header = headers.get('Date')
            d_time = utils.parse_timestamp_string(date_header) if date_header is not None else None
        else:
            d_time = None
        datum = message[0].get(self.point_name)
        if datum:
            self.set_meter_value(datum, d_time)
        else:
            _log.warning('Received bad message from {} on topic {}, bus {} from peer {}. Message: {} Headers: {}'
                         .format(sender, topic, bus, peer, message, headers))

    def store(self):
        _log.warning('MeterPoint.store() is not implemented for VOLTTRON. Use historian.')

    @classmethod
    def get_meters_by_hook(cls, meter_list, hook):
        return [m for m in meter_list if m.hook == hook and isinstance(m, cls)]

    @classmethod
    def get_meter_by_hook(cls, meter_list, hook):
        return PushMeterPoint.get_meters_by_hook(meter_list, hook)[0]
