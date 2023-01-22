"""
Copyright (c) 2022, Battelle Memorial Institute
All rights reserved.
Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:
1. Redistributions of source code must retain the above copyright notice, this
   list of conditions and the following disclaimer.
2. Redistributions in binary form must reproduce the above copyright notice,
   this list of conditions and the following disclaimer in the documentation
   and/or other materials provided with the distribution.
THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
The views and conclusions contained in the software and documentation are those
of the authors and should not be interpreted as representing official policies,
either expressed or implied, of the FreeBSD Project.
This material was prepared as an account of work sponsored by an agency of the
United States Government. Neither the United States Government nor the United
States Department of Energy, nor Battelle, nor any of their employees, nor any
jurisdiction or organization that has cooperated in th.e development of these
materials, makes any warranty, express or implied, or assumes any legal
liability or responsibility for the accuracy, completeness, or usefulness or
any information, apparatus, product, software, or process disclosed, or
represents that its use would not infringe privately owned rights.
Reference herein to any specific commercial product, process, or service by
trade name, trademark, manufacturer, or otherwise does not necessarily
constitute or imply its endorsement, recommendation, or favoring by the
United States Government or any agency thereof, or Battelle Memorial Institute.
The views and opinions of authors expressed herein do not necessarily state or
reflect those of the United States Government or any agency thereof.

PACIFIC NORTHWEST NATIONAL LABORATORY
operated by BATTELLE for the UNITED STATES DEPARTMENT OF ENERGY
under Contract DE-AC05-76RL01830
"""

import logging
from volttron.platform.agent import utils
import numpy as np

_log = logging.getLogger(__name__)
utils.setup_logging()
OAT = "OutdoorAirTemperature"
CSP = "ZoneCoolingTemperatureSetPoint"
HSP = "HSP"
TIN = "ZoneTemperature"


class Thermostat(object):
    def __init__(self, config, **kwargs):
        self.name = "Thermostat"
        self.c1 = config["c1"]
        self.c2 = config["c2"]
        self.c3 = config["c3"]
        self.c4 = config["c4"]
        self.nominal_set_point = config.get('nominal_setpoint', 22.8)
        self.max_set_point_offset = config.get('max_set_point_offset', 2.0)
        self.oat = config.get("oat", 0.)
        self.csp = config.get("csp", 22.8)
        self.room_temp = config.get("room_temp", 22.8)
        self.current_time = None
        self.coefficients = {"c1", "c2", "c3", "c4"}
        self.rated_power = config["rated_power"]
        self.n_points = config.get("demand_curve_points", 2)
        self.topic = config.get("topic", None)
        self.error = False
        self.actuation_topic = config.get('actuation_topic', None)

    def predict_flexibility(self, params=None):
        min_set_point, max_set_point = self.set_point_range()
        csp_flex = np.linspace(min_set_point, max_set_point, num=self.n_points)
        # _log.debug(f'csp_flex is: {csp_flex}')
        return [self.predict_power(params, csp) for csp in csp_flex]

    def predict_power(self, params=None, set_point=None):
        params = params if params else {}
        csp = set_point if set_point else self.csp
        oat = self.oat if not params.get(OAT) else params.get(OAT)
        temp = self.room_temp if not params.get(TIN) else params.get(TIN)
        index = self.current_time.hour if not params.get('interval_time') else params.get('interval_time').hour
        duty_cycle = min([1, max([0, self._get_q(oat, temp, csp, index)])])
        power = duty_cycle * self.rated_power
        return -power

    def set_point_range(self, interval_start_time=None):
        set_point = self.nominal_set_point  # TODO: Extend for time_based schedule.
        return set_point - self.max_set_point_offset, set_point + self.max_set_point_offset

    def update_data(self, data, now):
        """
        Update current data measurements.
        """
        try:
            self.oat = data[OAT]
            self.csp = data[CSP]
            self.room_temp = data[TIN]
            self.current_time = now
            self.error = False
        except KeyError:
            _log.debug("Error for %s input data on topic %s", self.name, self.topic)
            self.error = True

    def _get_q(self, oat, temp, temp_stpt, index):
        # _log.debug(f'oat: {oat}, temp: {temp}, temp_stpt: {temp_stpt}, index: {index}')
        q = temp_stpt * self.c1[index] + temp * self.c2[index] + oat * self.c3[index] + self.c4[index]
        return q