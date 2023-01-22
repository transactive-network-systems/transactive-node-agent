import importlib
import logging
import weakref

from typing import Dict

from tent.local_asset import LocalAsset
from tent.transactive_node import TransactiveNode
from tent.utils.log import setup_logging

setup_logging()
_log = logging.getLogger(__name__)


class ActuationManager:
    def __init__(self,
                 active_onstart: bool = True,
                 control_interval: float = 60,
                 #outputs: Dict[dict] = None,  # TODO: How is this handled in parent?
                 parent: LocalAsset = None,
                 occupancy_manager: dict = None,
                 transactive_node: TransactiveNode = None):
        self.active_onstart = active_onstart
        self.control_interval = control_interval
        #self.outputs = outputs if outputs else []
        self.parent = parent if parent else LocalAsset()
        self.tn = None if transactive_node is None else weakref.ref(transactive_node)

        # Initialize Scheduler
        if occupancy_manager:
            om_class = occupancy_manager.pop('class_name', 'OccupancyManager')
            om_module = occupancy_manager.pop('module_name', 'transactive_node.local_asset.occupancy_manager')
            module = importlib.import_module(om_module)
            cls = getattr(module, om_class)
            self.occupancy_manager = cls(**occupancy_manager)

        self.actuation_active = False
        self.actuation_allowed = True if self.active_onstart else False
        self.actuation_periodic = None

        # if self.outputs:
        #     if self.tn and self.tn():
        #         tn = self.tn()
        #         tn.vip.pubsub.subscribe(peer='pubsub', prefix=self.enable_topic, callback=self.update_actuation_state)
        #     if self.active_onstart:
        #         self.update_actuation_state(True)
        # else:
        #     _log.info(f"{self.parent.name} - cannot initialize actuation state, no configured outputs.")

    def actuate(self, mkt):
        start_time = mkt.marketClearingTime + mkt.deliveryLeadTime
        end_time = start_time + mkt.intervalDuration
        if (self.occupancy_manager.check_schedule(start_time)
                and self.occupancy_manager.check_schedule(end_time)
                and self.actuation_allowed):
            self.actuation_active = True
        else:
            self.actuation_active = False

    # TODO: VOLTTRON Specific, and possibly unneeded?
    # def enable_callback(self, peer, sender, bus, topic, headers, message):
    #     state = message
    #     _log.debug(f"{self.parent.name} actuation disabled {not bool(state)}")
    #     if not self.actuation_allowed and not (bool(state)):
    #         return
    #     self.actuation_allowed = bool(state)
    #     self.update_actuation_state(state)
