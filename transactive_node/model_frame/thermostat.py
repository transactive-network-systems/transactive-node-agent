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
OAT = "OAT"
CSP = "CSP"
HSP = "HSP"
TIN = "TIN"


class Thermostat(object):
    def __init__(self, config, parent, **kwargs):
        self.parent = parent
        self.name = "Thermostat"
        self.c1 = config["c1"]
        self.c2 = config["c2"]
        self.c3 = config["c3"]
        self.c4 = config["c4"]
        self.oat = 0.
        self.csp = 0
        self.room_temp = 0
        self.current_time = None
        self.coefficients = {"c1", "c2", "c3", "c4"}
        self.rated_power = config["rated_power"]
        self.n_points = config.get("demand_curve_points")
        self.topic = config.get("topic", None)
        self.error = False

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
        except KeyError as ex:
            _log.debug("Error for %s input data on topic %s", self.name, self.topic)
            self.error = True

    def predict_flexibility(self, params=None):
        csp_flex = np.linspace(self.csp - 2, self.csp + 2, num=self.n_points)
        q = []
        for csp in csp_flex:
            prediction = self.predict_power(params, csp)
            q.append(prediction)
        return q

    def predict_power(self, params=None, set_point=None):
        csp = set_point if set_point else self.csp
        oat = self.oat if not params.get(OAT) else params.get(OAT)
        temp = self.room_temp if not params(TIN) else params.get(TIN)
        index = self.current_time.hour if not params('interval_time') else params.get('interval_time').hour
        power = self.get_q(oat, temp, csp, index)
        return power

    def get_q(self, oat, temp, temp_stpt, index):
        q = temp_stpt * self.c1[index] + temp * self.c2[index] + oat * self.c3[index] + self.c4[index]
        return q
