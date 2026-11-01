import logging

from collections.abc import Mapping
from datetime import datetime

from tent.meter_point.push_meter_point import PushMeterPoint
from tent.utils.helpers import validate_bool

_log = logging.getLogger(__name__)


class TNSMeterPoint(PushMeterPoint):
    """A meter point fed by a VOLTTRON device publication: one named point out of the published all-points mapping."""

    def __init__(self, subscribe_key: str, point_name: str, external_platform: bool, *args, **kwargs):
        # subscribe_key should be a string which is mapped to a topic in the TNS agent configuration "topics" dict.
        self.point_name = str(point_name)
        self.external_platform = validate_bool(external_platform, 'external_platform')
        super().__init__(subscribe_key=subscribe_key, *args, **kwargs)

    def handle_subscription(self, time_received: datetime, subscribe_key: str, message: Mapping):
        if subscribe_key != self.subscribe_key:
            _log.warning(f'Received unexpected subscribe_key "{subscribe_key}" in MeterPoint expecting '
                         f'"{self.subscribe_key}"; ignored.')
            return
        if not isinstance(message, Mapping):
            _log.warning(f'Meter point {self.name!r} expected a mapping of point names to values, '
                         f'got {type(message).__name__}: {message!r}')
            return
        datum = message.get(self.point_name)
        if datum is None:
            _log.warning(f'Meter point {self.name!r} found no value for point {self.point_name!r} in message '
                         f'with points {sorted(message)}')
            return
        # In simulation the reading is stamped with the simulated clock (set_meter_value's default), since the
        # publication's real-time header has no meaning on the simulated axis.
        d_time = None if (self.tn and self.tn.simulation) else time_received
        self.set_meter_value(datum, d_time)
