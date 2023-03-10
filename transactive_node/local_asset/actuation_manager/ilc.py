import logging

from datetime import datetime
from typing import Union

from tent.utils.helpers import format_timestamp
from tent.utils.log import setup_logging
from tent.utils.timer import Timer

from transactive_node.local_asset.actuation_manager.external_target import ExternalTargetActuationManager

setup_logging()
_log = logging.getLogger(__name__)


class ILCActuationManager(ExternalTargetActuationManager):
    def __init__(self, ilc_target_topic, **kwargs):
        super(ILCActuationManager, self).__init__(**kwargs)
        self.ilc_target_topic = ilc_target_topic

    def set_target(self, target: Union[float, None], start: datetime, end: datetime):
        target_id: str = f'{self.parent.name}_{start}'
        tn = self.tn()
        headers = {
            'Timestamp': format_timestamp(Timer.now()),
            'Datetime': format_timestamp(Timer.now())
        }
        target = [
            {
                "value": {
                    "id": target_id,
                    "target": -target if target is not None else None,
                    "start": format_timestamp(start),
                    "end": format_timestamp(end)
                }
            },
            {
                "value": {
                    "units": "kW",
                    "tz": str(tn.tz)
                }
            }
        ]
        try:
            tn.vip.pubsub.publish(peer='pubsub', topic=self.ilc_target_topic,
                                  headers=headers, message=target)
        except Exception as e:
            _log.debug(f'Error publishing to target: {e}')

    def release_target(self, start, end):
        self.set_target(None, start, end)
