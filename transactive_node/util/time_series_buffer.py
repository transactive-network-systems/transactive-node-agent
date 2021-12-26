# Copyright 2019 The University of Toledo
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from collections import deque, namedtuple
from typing import Union, Iterable
import pytz
from datetime import datetime, timedelta, tzinfo

PointRecord = namedtuple('PointRecord', ['value', 'd_time'])


# TODO: Test dt_class functionality with util.Timer.
# TODO: Implement comparison methods (__lt__, __eq__, etc)
class TimeSeriesBuffer(deque):
    def __init__(self, iterable: Iterable = (), maxlen: int = None, tz: Union[tzinfo, str] = 'UTC',
                 dt_class: datetime = datetime, expiry: timedelta = None):
        super(TimeSeriesBuffer, self).__init__(iterable, maxlen)
        self.tz = tz if isinstance(tz, tzinfo) else pytz.timezone(tz)
        self.last = None
        self.dt_class = dt_class
        self.expiry = expiry

    def append(self, value, d_time=None):
        if not isinstance(value, PointRecord):
            d_time = d_time if d_time else self.dt_class.now(pytz.utc).astimezone(self.tz)
            value = PointRecord(value, d_time)
        self._prune_expired()
        if not self._expired(value):
            self.last = value
            super(TimeSeriesBuffer, self).append(value)

    def appendleft(self, value, d_time=None):
        if not isinstance(value, PointRecord):
            d_time = d_time if d_time else self.dt_class.now(pytz.utc).astimezone(self.tz)
            value = PointRecord(value, d_time)
        self._prune_expired()
        if not self._expired(value):
            super(TimeSeriesBuffer, self).appendleft(value)

    def extend(self, values):
        if not all(isinstance(x, PointRecord) for x in values):
            if all(len(x) == 2 and isinstance(x[1], datetime) for x in values):
                values = [PointRecord(*x) for x in values]
            else:
                raise ValueError('Values must be iterable and all elements must be compatible with PointRecord.')
        values.sort(key=lambda x: x[1])
        values = list(filter(lambda x: x[1] + self.expiry > self.dt_class.now(pytz.utc).astimezone(self.tz), values))
        if len(values) > 0:
            self._prune_expired()
            self.last = values[-1]
            super(TimeSeriesBuffer, self).extend(values)

    def extendleft(self, values):
        if not all(isinstance(x, PointRecord) for x in values):
            if all(len(x) == 2 and isinstance(x[1], datetime)for x in values):
                values = [PointRecord(*x) for x in values]
            else:
                raise ValueError('Values must be iterable and all elements must be compatible with PointRecord.')
        values.sort(key=lambda x: x[1], reverse=True)
        values = list(filter(lambda x: x[1] + self.expiry > self.dt_class.now(pytz.utc).astimezone(self.tz), values))
        if len(values) > 0:
            super(TimeSeriesBuffer, self).extendleft(values)

    def get(self, since=None, until=None, columns=False):
        self._prune_expired()
        retval = self
        retval = self._since(retval, since) if since else retval
        retval = self._until(retval, until) if until else retval
        retval = list(zip(*retval)) if columns else list(retval)
        return retval

    def get_values(self, since=None, until=None):
        retval = self.get(since, until, columns=True)
        return retval[0] if retval else []

    def get_times(self, since=None, until=None):
        retval = self.get(since, until, columns=True)
        return retval[1] if retval else []

    @staticmethod
    def _until(inval, until):
        if not isinstance(until, datetime):
            raise ValueError("If specified, until must be a datetime")
        return filter(lambda d: d[1] <= until, inval)

    @staticmethod
    def _since(inval, since):
        if not isinstance(since, datetime):
            raise ValueError("If specified, since must be a datetime.")
        return filter(lambda d: d[1] >= since, inval)

    def _expired(self, element):
        if not self.expiry:
            return False
        return True if element[1] + self.expiry <= self.dt_class.now(pytz.utc).astimezone(self.tz) else False

    def _prune_expired(self):
        if len(self) == 0 or not self.expiry:
            return
        now = self.dt_class.now(pytz.utc).astimezone(self.tz)
        while self[0][1] + self.expiry <= now:
            self.popleft()
        while self[-1][1] + self.expiry <= now:
            self.pop()

    maxlen = property(lambda self: object(), lambda self, v: None, lambda self: None)
