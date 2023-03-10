from datetime import datetime
from dateutil import parser

from tent.utils.timer import Timer


class OccupancyManager:
    def __init__(self, schedule: dict):
        self.occupied = property(self.check_schedule)
        self.schedule = {}
        if schedule:
            self.always_occupied = False
            for day_str, schedule_info in schedule.items():
                _day = parser.parse(day_str).weekday()
                if schedule_info not in ["always_on", "always_off"]:
                    start = parser.parse(schedule_info["start"]).time()
                    end = parser.parse(schedule_info["end"]).time()
                    self.schedule[_day] = {"start": start, "end": end}
                else:
                    self.schedule[_day] = schedule_info
        else:
            self.always_occupied = True

    def check_schedule(self, dt: datetime = None):
        if self.always_occupied:
            return True
        dt = dt if dt else Timer.now()
        current_schedule = self.schedule[dt.weekday()]
        if "always_on" in current_schedule:
            occupied = True
        elif "always_off" in current_schedule:
            occupied = False
        else:
            _start = current_schedule["start"]
            _end = current_schedule["end"]
            occupied = True if _start <= dt.time() < _end else False
        return occupied
