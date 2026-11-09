import logging

from datetime import datetime
from typing import Union

from tent.config import ConfigModel, Setting
from tent.utils.log import setup_logging
from tent.utils.timer import Timer
from tent.local_asset.actuation_manager.external_target import ExternalTargetActuationManager

setup_logging()
_log = logging.getLogger(__name__)


class ILCActuationManager(ExternalTargetActuationManager):
    """Hands each interval's scheduled power to an ILC agent as a demand target, published on `ilc_target_topic` through
    the node's publication queue like every other publication of the agent."""

    class Config(ExternalTargetActuationManager.Config):
        ilc_target_topic: str = Setting(..., min_length=1, description='Topic the ILC agent reads demand targets from.')

    def __init__(self, parent=None, occupancy_manager=None, transactive_node=None, config: ConfigModel | None = None,
                 **kwargs):
        if transactive_node is None:
            raise ValueError('ILCActuationManager requires transactive_node: targets are published through it.')
        cfg = self._configure(config, kwargs)
        super(ILCActuationManager, self).__init__(parent=parent, occupancy_manager=occupancy_manager,
                                                  transactive_node=transactive_node, config=cfg)
        self.ilc_target_topic: str = cfg.ilc_target_topic

    def set_target(self, target: Union[float, None], start: datetime, end: datetime):
        target_id: str = f'{self.parent.name}_{start}'
        now = Timer.now().isoformat()
        headers = {
            'Timestamp': now,
            'Datetime': now
        }
        target_message = [
            {
                "value": {
                    "id": target_id,
                    "target": -target if target is not None else None,
                    "start": start.isoformat(),
                    "end": end.isoformat()
                }
            },
            {
                "value": {
                    "units": "kW",
                    "tz": str(self.tn.tz)
                }
            }
        ]
        _log.info(f'Setting ILC target to {target} between {start.isoformat()} and {end.isoformat()}')
        self.tn.publish_to_topic(self.ilc_target_topic, headers, target_message)

    def release_target(self, start, end):
        self.set_target(None, start, end)
