import gevent
import logging

from tent.utils.log import setup_logging

from transactive_node.local_asset.actuation_manager.direct_ratio import DirectRatioActuationManager

from volttron.platform.jsonrpc import RemoteError
from volttron.platform.vip.agent import errors

setup_logging()
_log = logging.getLogger(__name__)


class TNSDirectRatioActuationManager(DirectRatioActuationManager):
    def __init__(self, actuator_identity, **kwargs):
        super(TNSDirectRatioActuationManager, self).__init__(**kwargs)
        self.actuator_identity = actuator_identity

    def direct_actuate(self, target_id, new_set_point):
        if self.tn and self.tn():
            tn = self.tn()
            try:
                tn.vip.rpc.call(self.actuator_identity,
                                'set_point',
                                requester_id=self.parent.name,
                                topic=target_id,
                                value=new_set_point).get(timeout=15)
            except (RemoteError, gevent.Timeout, errors.VIPError) as ex:
                _log.warning(f"Failed to set {target_id} - ex: {str(ex)}")
