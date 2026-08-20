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
    def __init__(self, actuator_identity, **kwargs):
        super(TNSDirectRatioActuationManager, self).__init__(**kwargs)
        self.actuator_identity = actuator_identity

    def direct_actuate(self, target_id, new_set_point):
        _log.debug(f'Setting {target_id} to: {new_set_point}.')
        try:
            target_topic = target_id # TODO: self.tn.topics.get(target_id)
            if target_topic:
                self.tn.vip.rpc.call(self.actuator_identity,
                                'set_point',
                                requester_id=self.parent.name,
                                topic=target_topic,
                                value=new_set_point).get(timeout=15)
            except (RemoteError, gevent.Timeout, errors.VIPError) as ex:
                _log.warning(f"Failed to set {target_id} - ex: {str(ex)}")

    def direct_release(self, target_id):
        try:
            self.tn.vip.rpc.call(self.actuator_identity,
                            'revert_point',
                            requester_id=self.parent.name,
                            topic=target_id).get(timeout=15)
        except (RemoteError, gevent.Timeout, errors.VIPError) as ex:
            _log.warning(f"Failed to revert {target_id} - ex: {str(ex)}")
