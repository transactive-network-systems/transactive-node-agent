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
import time

from tent.config import ConfigModel, Setting
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

    The base class drives one point at a time (direct_actuate and direct_release per model). Each RPC round trip to the
    EnergyPlus agent takes a good part of a second, and an office has twenty points, so the points of one market are
    sent together: while the base class iterates, the calls are collected, then all of them are issued at once and the
    replies collected with one overall `rpc_timeout`. A failed or unanswered point is logged on its own; the others
    still complete. Which points are set, and to what, is unchanged; the order of log lines may differ.
    """

    class Config(DirectRatioActuationManager.Config):
        actuator_identity: str = Setting(..., min_length=1, reconfigurable=True,
                                         description='VIP identity of the agent answering set_point and revert_point.')
        rpc_timeout: float = Setting(15.0, gt=0, reconfigurable=True,
                                     description='[s] wait for all of a market\'s RPC replies together.')

    def __init__(self, parent=None, occupancy_manager=None, transactive_node=None, config: ConfigModel | None = None,
                 **kwargs):
        cfg = self._configure(config, kwargs)
        super(TNSDirectRatioActuationManager, self).__init__(parent=parent, occupancy_manager=occupancy_manager,
                                                             transactive_node=transactive_node, config=cfg)
        self.actuator_identity: str = cfg.actuator_identity
        self.rpc_timeout: float = cfg.rpc_timeout
        self._pending_sets: list | None = None      # (target_id, value) collected while the base class iterates
        self._pending_reverts: list | None = None   # target_id collected while the base class iterates

    @staticmethod
    def split_point(target_id: str) -> tuple[str, str]:
        """(device path, point name) of an actuation key such as Campus/SMALL_OFFICE_DR/HP1/ZoneCoolingTemperatureSetPoint."""
        path, separator, point_name = str(target_id).strip('/').rpartition('/')
        if not separator or not path or not point_name:
            raise ValueError(f'An actuation key must be <device path>/<point name>, got {target_id!r}.')
        return path, point_name

    # --------------------------------------------------------------------------------- the base class's per-point calls

    def actuate(self, mkt):
        self._pending_sets = []
        try:
            super(TNSDirectRatioActuationManager, self).actuate(mkt)
        finally:
            pending, self._pending_sets = self._pending_sets, None
        if pending:
            self.set_points(pending)

    def release(self, mkt):
        self._pending_reverts = []
        try:
            super(TNSDirectRatioActuationManager, self).release(mkt)
        finally:
            pending, self._pending_reverts = self._pending_reverts, None
        if pending:
            self.revert_points(pending)

    def direct_actuate(self, target_id, new_set_point):
        if self._pending_sets is not None:
            self._pending_sets.append((target_id, new_set_point))
        else:
            self.set_points([(target_id, new_set_point)])

    def direct_release(self, target_id):
        if self._pending_reverts is not None:
            self._pending_reverts.append(target_id)
        else:
            self.revert_points([target_id])

    # ---------------------------------------------------------------------------------------- the concurrent batches

    def set_points(self, targets: list[tuple[str, float]]):
        """Issue set_point for every (actuation key, value) at once, then collect the replies within rpc_timeout."""
        issued = []
        for target_id, value in targets:
            path, point_name = self.split_point(target_id)
            _log.debug(f'Setting {path}/{point_name} to {value} through {self.actuator_identity}.')
            try:
                result = self.tn.vip.rpc.call(self.actuator_identity, 'set_point', path, point_name, value)
            except (RemoteError, errors.VIPError) as error:
                self._report(f'set {path}/{point_name} to {value}', error)
                continue
            issued.append((f'{path}/{point_name}', value, result))
        deadline = time.monotonic() + self.rpc_timeout
        for point, value, result in issued:
            try:
                result.get(timeout=max(0.0, deadline - time.monotonic()))
            except (RemoteError, gevent.Timeout, errors.VIPError) as error:
                self._report(f'set {point} to {value}', error)

    def revert_points(self, targets: list[str]):
        """Issue revert_point for every actuation key at once, then collect the replies within rpc_timeout; a reply
        other than SUCCESS is an error."""
        issued = []
        for target_id in targets:
            path, point_name = self.split_point(target_id)
            try:
                result = self.tn.vip.rpc.call(self.actuator_identity, 'revert_point', path, point_name)
            except (RemoteError, errors.VIPError) as error:
                self._report(f'revert {path}/{point_name}', error)
                continue
            issued.append((f'{path}/{point_name}', result))
        deadline = time.monotonic() + self.rpc_timeout
        for point, result in issued:
            try:
                reply = result.get(timeout=max(0.0, deadline - time.monotonic()))
            except (RemoteError, gevent.Timeout, errors.VIPError) as error:
                self._report(f'revert {point}', error)
                continue
            if reply != 'SUCCESS':
                _log.error(f'Actuator {self.actuator_identity} did not revert {point}: reply {reply!r}.')

    def _report(self, what: str, error: Exception):
        _log.error(f'Failed to {what} through {self.actuator_identity}: {type(error).__name__}: {error}')
