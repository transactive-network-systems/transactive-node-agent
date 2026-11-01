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
import gevent
import logging

from tent.utils.log import setup_logging
from tent.local_asset.actuation_manager.direct_ratio import DirectRatioActuationManager

from importlib.metadata import distribution, PackageNotFoundError
try:
    distribution('volttron-core')
    from volttron.client.vip.agent import errors
    from volttron.utils.jsonrpc import RemoteError
except PackageNotFoundError:
    from volttron.platform.jsonrpc import RemoteError
    from volttron.platform.vip.agent import errors

setup_logging()
_log = logging.getLogger(__name__)


class TNSDirectRatioActuationManager(DirectRatioActuationManager):
    """Writes each model's set point through an actuator agent's RPC interface (the platform actuator or the EnergyPlus
    agent installed as one). A model's `actuation_key` is the point's full topic, `<device path>/<point name>`; the RPC
    methods take the device path and the point name separately, so the key is split at its last slash.

    Calls: `set_point(path, point_name, value)`, which returns the value written and raises for an unknown point or a
    failed write; `revert_point(path, point_name)`, which returns 'SUCCESS' or 'FAILURE'.
    """

    def __init__(self, actuator_identity: str, rpc_timeout: float = 15.0, **kwargs):
        if not actuator_identity:
            raise ValueError('TNSDirectRatioActuationManager requires actuator_identity, the actuator agent\'s VIP identity.')
        super(TNSDirectRatioActuationManager, self).__init__(**kwargs)
        self.actuator_identity = str(actuator_identity)
        self.rpc_timeout = float(rpc_timeout)

    @staticmethod
    def split_point(target_id: str) -> tuple[str, str]:
        """(device path, point name) of an actuation key such as Campus/SMALL_OFFICE_DR/HP1/ZoneCoolingTemperatureSetPoint."""
        path, separator, point_name = str(target_id).strip('/').rpartition('/')
        if not separator or not path or not point_name:
            raise ValueError(f'An actuation key must be <device path>/<point name>, got {target_id!r}.')
        return path, point_name

    def direct_actuate(self, target_id, new_set_point):
        path, point_name = self.split_point(target_id)
        _log.debug(f'Setting {path}/{point_name} to {new_set_point} through {self.actuator_identity}.')
        try:
            self.tn.vip.rpc.call(self.actuator_identity, 'set_point', path, point_name,
                                 new_set_point).get(timeout=self.rpc_timeout)
        except (RemoteError, gevent.Timeout, errors.VIPError) as error:
            _log.error(f'Failed to set {path}/{point_name} to {new_set_point} through {self.actuator_identity}: '
                       f'{type(error).__name__}: {error}')

    def direct_release(self, target_id):
        path, point_name = self.split_point(target_id)
        try:
            reply = self.tn.vip.rpc.call(self.actuator_identity, 'revert_point', path,
                                         point_name).get(timeout=self.rpc_timeout)
        except (RemoteError, gevent.Timeout, errors.VIPError) as error:
            _log.error(f'Failed to revert {path}/{point_name} through {self.actuator_identity}: '
                       f'{type(error).__name__}: {error}')
            return
        if reply != 'SUCCESS':
            _log.error(f'Actuator {self.actuator_identity} did not revert {path}/{point_name}: reply {reply!r}.')
