"""
Copyright (c) 2020, Battelle Memorial Institute
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

import importlib
import logging
import numpy as np

from datetime import datetime
from typing import Iterable, List

from volttron.platform.agent import utils

_log = logging.getLogger(__name__)
utils.setup_logging()
__version__ = "0.1"

__all__ = ['ModelFrame']


class ModelFrame(object):
    def __init__(self, config, **kwargs):
        self.models = {}
        self.cleared_quantity = None
        if not config:
            return
        self.demand_curve_points = config.get('demand_curve_points', 2)
        base_module = "transactive_node.model_frame."
        for model in config['models']:
            try:
                topic = model["topic"]
                model_type = model["model_type"]
            except KeyError as e:
                _log.exception("Missing Model Type key: {}".format(e))
                raise e
            _file, model_type = model_type.split(".")
            module = importlib.import_module(base_module + _file)
            self.model_class = getattr(module, model_type)
            model['model_config']['demand_curve_points'] = self.demand_curve_points
            self.models[topic] = self.model_class(model['model_config'], parent=self)

    def model_flexibility(self, params: dict = None) -> List[float]:
        params = params if params else {}
        q = np.zeros((len(self.models), self.demand_curve_points))
        for i, model in enumerate(self.models.values()):
            try:
                q[i, :] = model.predict_flexibility(params=params)
            except KeyError:
                _log.debug("Error making prediction for %s", model.topic)
        flexibility = q.sum(axis=0)
        return list(flexibility)

    def model_power(self, params: dict = None) -> float:
        params = params if params else {}
        q = 0.0
        for k, model in self.models.items():
            try:
                q += model.predict_power(params=params, set_point=None)
                # _log.debug(f'Power after {k}: {q}')
            except KeyError:
                _log.debug("Error making prediction for %s", model.topic)
        return q

#
# class BaseModel(object):
#     def __init__(self, actuation_topic: str = None):
#         self.topic = actuation_topic
#
#     def predict_flexibility(self, params: dict = None) -> Iterable[float]:
#         pass
#
#     def predict_power(self, params: dict = None, set_point: float = None) -> float:
#         pass
#
#     def set_point_range(self, interval_start_time: datetime = None) -> Iterable[float]:
#         pass
#
#     def update_data(self, data: dict, now: datetime) -> None:
#         pass