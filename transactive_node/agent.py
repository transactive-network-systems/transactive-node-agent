import logging
import importlib
import re

import gevent
import pytz
from dateutil import parser
from tzlocal import get_localzone
from datetime import timedelta

from volttron.platform.agent import utils
from volttron.platform.vip.agent import Agent

from ...TNT_Version3.PyCode.TransactiveNode import TransactiveNode
from ...TNT_Version3.PyCode.market_state import MarketState
from ...TNT_Version3.PyCode.timer import Timer

utils.setup_logging()
_log = logging.getLogger(__name__)
__version__ = '0.1'


class TransactiveNodeAgent(Agent, TransactiveNode):
    """Transactive Node Agent.

    This agent acts as a single node in a transactive network.
    It wraps the TNS.myTransactiveNode class.
    The Transactive Node Agent handles configuration of the node and its services,
    provides access to messages on the VOLTTRON message bus, and runs the loop.
    """
    def __init__(self, **kwargs):
        Agent.__init__(self, **kwargs)
        TransactiveNode.__init__(self)

        # Initial State:
        self._stop_agent = False

        # Default configuration.
        self.db_topic = 'TNS'
        self.transactive_record_topic = f'{self.db_topic}/{self.name}/transactive_record'
        self.subscribe_all_platforms = False
        self.tz = get_localzone()  # TODO: The Timer does not use aware date-times. This should be fixed.
        self.run_gap = timedelta(minutes=10)  # Do not run if next scheduled run is closer than this (seconds)
        self.plots_active = False  # TODO: Do the plots actually belong in the agent code, if not where?

        self.simulation = False
        self.simulation_start_time = Timer.get_cur_time()
        self.simulation_one_hour_in_seconds = 3600

        # TODO: Add configuration in appropriate dependency class (probably ConsensusMarket):
        #  self.reschedule_interval = timedelta(minutes=10, seconds=1)

        # Set up config store.
        self.default_config = {
            # TransactiveNode Configurations (from TransactiveNode super-class):
            "description": self.description,
            "mechanism": self.mechanism,
            "name": self.name,
            "status": self.status,

            # Dependency Objects (from TransactiveNode super-class):
            "meterPoints": self.meterPoints,
            "informationServiceModels": self.informationServiceModels,
            "localAssets": self.localAssets,
            "markets": self.markets,
            "neighbors": self.neighbors,

            # Agent Configurations:
            "db_topic": self.db_topic,
            "transactive_record_topic": self.transactive_record_topic,
            "subscribe_all_platforms": self.subscribe_all_platforms,
            "tz": self.tz,
            "run_gap": self.run_gap.total_seconds(),  # TODO: Should this be a property of the market?
            "plots_active": self.plots_active,

            # Simulation Parameters:
            "simulation": self.simulation,
            "simulation_start_time": utils.format_timestamp(self.simulation_start_time),
            "simulation_one_hour_in_seconds": self.simulation_one_hour_in_seconds

            # TODO: Add configuration in appropriate dependency class (probably ConsensusMarket):
            #  "reschedule_interval": self.reschedule_interval.total_seconds(),
        }
        self.vip.config.set_default("config", self.default_config)
        self.vip.config.subscribe(self.configure_main, actions=["NEW", "UPDATE"], pattern="config")

    def configure_main(self, config_name, action, contents):
        _log.info('Received configuration {} signal: {}'.format(action, config_name))
        self.vip.pubsub.unsubscribe("pubsub", None, None)
        config = self.default_config.copy()
        config.update(contents)
        # Agent Configurations:
        self.db_topic = config.get('db_topic', self.db_topic)
        self.transactive_record_topic = config.get('transactive_record_topic',
                                                   f'{self.db_topic}/{config.get("name")}/transactive_record')
        self.subscribe_all_platforms = bool(config.get('subscribe_all_platforms', self.subscribe_all_platforms))
        self.tz = pytz.timezone(config.get('tz', str(self.tz)))
        self.run_gap = timedelta(seconds=float(config.get('run_gap', self.run_gap.total_seconds())))
        self.plots_active = bool(config.get('plots_active', self.plots_active))

        # Simulation Configurations:
        self.simulation = bool(config.get('simulation', self.simulation))
        self.simulation_start_time = parser.parse(config.get('simulation_start_time', self.simulation_start_time))
        self.simulation_one_hour_in_seconds = int(config.get('simulation_one_hour_in_seconds',
                                                             self.simulation_one_hour_in_seconds))

        # TODO: Move these into appropriate dependency class (probably ConsensusMarket):
        #  reschedule_interval = float(config.get('reschedule_interval'))
        #  self.reschedule_interval = timedelta(seconds=reschedule_interval) if reschedule_interval \
        #   else self.reschedule_interval

        # Configure TransactiveNode object
        try:
            self.description = config.get('description', self.description)
            self.mechanism = config.get('mechanism', self.mechanism)
            self.name = config.get('name', self.name)
            self.status = config.get('status', self.status)

            self.meterPoints = self.configure_dependencies(config.get('meterPoints'), 'MeterPoints')
            self.informationServiceModels = self.configure_dependencies(config.get('informationServiceModels'),
                                                                        'InformationServiceModels')
            self.localAssets = self.configure_dependencies(config.get('localAssets'), 'LocalAssets')
            self.markets = self.configure_dependencies(config.get('markets'), 'Markets')
            self.neighbors = self.configure_dependencies(config.get('neighbors'), 'Neighbors')
        except ValueError as e:
            _log.error("ERROR PROCESSING CONFIGURATION {}".format(e))
            raise

        for market in self.markets:
            if not market.upstairs_neighbor:  # TODO: Should this be checking the node for an upstairs neighbor?
                # Schedule to run 1st time now (or next interval if it is too close to the end of the current interval).
                next_exp_time = self.get_exp_start_time()
                next_analysis_time = next_exp_time
                if self.simulation:
                    next_analysis_time = self.simulation_start_time

                _log.debug("{} schedule to run at exp_time: {} analysis_time: {}".format(self.name,
                                                                                         next_exp_time,
                                                                                         next_analysis_time))
                self.core.spawn_later(5, self.state_machine_loop)

    def configure_dependencies(self, configs, dependency_type):
        """Configures each dependency in passed list of configurations.

        Instantiates each dependency.
        Requires each dependency have a configure() method which is passed a dictionary of configurations and
        a reference to this agent.
        It is up to the individual dependency to manage its own configuration.

        Returns list of instantiated and configured dependencies"""
        first_cap_re = re.compile('(.)([A-Z][a-z]+)')
        all_cap_re = re.compile('([a-z0-9])([A-Z])')

        def camel_to_snake(name):
            s1 = first_cap_re.sub(r'\1_\2', name)
            return all_cap_re.sub(r'\1_\2', s1).lower()

        dependencies = []
        if not configs:
            return dependencies
        for config in configs:
            cls = config.get('class_name')
            # TODO: What will be the default directory for classes in the TransactiveNodeAgent module?
            default_module_name = 'tns.' + camel_to_snake(cls)
            module = config.get('module', default_module_name)
            module = importlib.import_module(module)
            dependency = getattr(module, cls)()
            dependency.configure(self, config)
            dependencies.append(dependency)
        if len(dependencies) != len(set(dependencies)):
            raise ValueError(f'Configured {dependency_type} have duplicate names: {[d.name for d in dependencies]}')
        return dependencies

    def get_exp_start_time(self):
        one_second = timedelta(seconds=1)
        if self.simulation:
            next_exp_time = Timer.get_cur_time() + one_second
        else:
            now = Timer.get_cur_time()
            next_exp_time = now + self.run_gap
            if next_exp_time.hour == now.hour:
                next_exp_time = now + one_second
            else:
                _log.debug("{} did not run onstart because it's too late. Wait for next interval.".format(self.name))
                next_exp_time = next_exp_time.replace(minute=0, second=0, microsecond=0)
        return next_exp_time

    def state_machine_loop(self):
        # This is the entire timing logic. It relies on current market object's state machine method events()
        # TODO: Building agent has a small snippet before this which needs to be handled (somewhere).
        while not self._stop_agent:  # a condition may be added to provide stops or pauses.
            markets_to_remove = []
            for i in range(len(self.markets)):
                self.markets[i].events(self)
                # _log.debug("Markets: {}, Market name: {}, Market state: {}".format(len(self.markets),
                #                                                                   self.markets[i].name,
                #                                                                   self.markets[i].marketState))

                if self.markets[i].marketState == MarketState.Expired:
                    markets_to_remove.append(self.markets[i])
                # NOTE: A delay may be added, but the logic of the market(s) alone should be adequate to drive system
                # activities
                gevent.sleep(0.01)
            for mkt in markets_to_remove:
                _log.debug("Market name: {}, Market state: {}. It will be removed shortly".format(mkt.name,
                                                                                                  mkt.marketState))
                self.markets.remove(mkt)
