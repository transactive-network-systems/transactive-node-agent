import logging
from datetime import timedelta

from volttron.platform.agent import utils

from ...TNT_Version3.PyCode.meter_point import MeterPoint, MeasurementType, MeasurementUnit

from util.time_series_buffer import TimeSeriesBuffer

utils.setup_logging()
_log = logging.getLogger(__name__)


class PushMeterPoint(MeterPoint, TimeSeriesBuffer):
    def __init__(self, maxlen: int = None):
        MeterPoint.__init__(self)
        TimeSeriesBuffer.__init__(self, maxlen=maxlen)
        super(PushMeterPoint, self).__init__()
        self.hook = None
        self.topic = None
        self.point_name = None
        self.external_platform = None
        self.scale_factor = 1  # Use -1 to reverse the sign of the meter.

    def configure(self, mtn, config):
        # Configure MeterPoint
        self.name = config.get('name', self.name)
        self.description = config.get('description', self.description)
        self.measurementInterval = timedelta(seconds=float(config.get('measurement_interval',
                                                                      self.measurementInterval.seconds)))
        self.measurementType = MeasurementType[config.get('measurement_type', self.measurementType.name)]
        self.measurementUnit = MeasurementUnit[config.get('measurement_units', self.measurementUnit.name)]

        # Configure PushMeterPoint
        self.hook = config.get('hook', self.hook)
        self.topic = config.get('topic', self.topic)
        self.point_name = config.get('point_name', self.point_name)
        self.external_platform = config.get('external_platform', self.external_platform)
        self.scale_factor = float(config.get('scale_factor', self.scale_factor))

        if self.topic:
            all_platforms = {'all_platforms': True} if self.external_platform else {}
            mtn.vip.pubsub.subscribe('pubsub', self.topic, self.on_topic, **all_platforms)

    def on_topic(self, peer, sender, bus, topic, headers, message):
        date_header = headers.get('Date')
        d_time = utils.parse_timestamp_string(date_header) if date_header is not None else None
        datum = self.scale_factor * message[0].get(self.point_name)

        if datum:
            self.append(datum, d_time)
            self.lastUpdate = d_time
        else:
            _log.warning('Received bad message from {} on topic {}, bus {} from peer {}. Message: {} Headers: {}'
                         .format(sender, topic, bus, peer, message, headers))

    def read_meter(self, obj):
        _log.warning('MeterPoint.read_meter() is not implemented for VOLTTRON. Use subscription for push updates.')

    def store(self):
        _log.warning('MeterPoint.store() is not implemented for VOLTTRON. Use historian.')

    @classmethod
    def get_meters_by_hook(cls, meter_list, hook):
        return [m for m in meter_list if m.hook == hook and isinstance(m, cls)]

    @classmethod
    def get_meter_by_hook(cls, meter_list, hook):
        return PushMeterPoint.get_meters_by_hook(meter_list, hook)[0]

    @classmethod
    def get_meters_by_name(cls, meter_list, name):
        return [m for m in meter_list if m.name == name and isinstance(m, cls)]

    @classmethod
    def get_meter_by_name(cls, meter_list, name):
        return PushMeterPoint.get_meters_by_name(meter_list, name)[0]
    
    @classmethod
    def get_meters_by_measurement_type(cls, meter_list, measurement_type):
        return [m for m in meter_list if m.measurement_type == measurement_type and isinstance(m, cls)]

    @classmethod
    def get_meter_by_measurement_type(cls, meter_list, measurement_type):
        return PushMeterPoint.get_meters_by_measurement_type(meter_list, measurement_type)[0]