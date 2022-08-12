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

import os
import sys
import logging
import datetime
import gevent
import importlib
import logging
import pytz
import re
import sys

from dateutil import parser
from tzlocal import get_localzone

from tent.enumerations.market_state import MarketState
from tent.transactive_node import TransactiveNode
from tent.utils.timer import Timer

from volttron.platform.agent import utils
from volttron.platform.vip.agent import Agent, Core

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
    def __init__(self, config_path=None, *args, **kwargs):
        _log.debug('in init')
        Agent.__init__(self, *args, **kwargs)
        _log.debug('Agent initialized')
        TransactiveNode.__init__(self)
        _log.debug('Node initialized')

        # Initial State:
        self._stop_agent = False

        # Default configuration.
        self.db_topic = 'TNS'
        self.transactive_operation_topic = f"{self.db_topic}/{self.name}/transactive_operation"
        self.transactive_record_topic = f'{self.db_topic}/{self.name}/transactive_record'
        self.local_asset_topic = f"{self.db_topic}/{self.name}/local_assets"
        self.market_balanced_price_topic = "{}/{}/market_balanced_prices".format(self.db_topic, self.name)
        self.subscribe_all_platforms = False
        self.tz = get_localzone()  # TODO: The Timer does not use aware date-times. This should be fixed.
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
            "transactive_operation_topic": self.transactive_operation_topic,
            "transactive_record_topic": self.transactive_record_topic,
            "subscribe_all_platforms": self.subscribe_all_platforms,
            "tz": self.tz,
            "plots_active": self.plots_active,
            "simulation": self.simulation,

            # TODO: Add configuration in appropriate dependency class (probably ConsensusMarket):
            #  "reschedule_interval": self.reschedule_interval.total_seconds(),
        }
        if self.simulation:
            self.default_config["simulation_start_time"] = utils.format_timestamp(self.simulation_start_time)
            self.default_config["simulation_one_hour_in_seconds"] = self.simulation_one_hour_in_seconds
        _log.debug('TN: before set_default')
        self.vip.config.set_default("config", self.default_config)
        _log.debug('TN: after set_default')
        self.vip.config.subscribe(self.configure_main, actions=["NEW", "UPDATE"], pattern="config")
        _log.debug('TN: end of init')

    def configure_main(self, config_name, action, contents):
        _log.info('Received configuration {} signal: {}'.format(action, config_name))
        self.vip.pubsub.unsubscribe("pubsub", None, None)
        config = self.default_config.copy()
        config.update(contents)

        # TransactiveNode Configurations:
        self.description = config.get('description', self.description)
        self.mechanism = config.get('mechanism', self.mechanism)
        self.name = config.get('name', self.name)
        self.status = config.get('status', self.status)

        # Agent Configurations:
        self.db_topic = config.get('db_topic', self.db_topic)
        self.transactive_operation_topic = f"{self.db_topic}/{self.name}/transactive_operation"
        self.transactive_record_topic = f'{self.db_topic}/{self.name}/transactive_record'
        self.local_asset_topic = f"{self.db_topic}/{self.name}/local_assets"
        self.market_balanced_price_topic = "{}/{}/market_balanced_prices".format(self.db_topic, self.name)
        self.subscribe_all_platforms = bool(config.get('subscribe_all_platforms', self.subscribe_all_platforms))
        self.tz = pytz.timezone(str(config.get('tz', self.tz)))
        self.plots_active = bool(config.get('plots_active', self.plots_active))

        # Simulation Configurations:
        self.simulation = bool(config.get('simulation', self.simulation))
        if self.simulation:
            self.simulation_start_time = parser.parse(config.get('simulation_start_time', self.simulation_start_time))
            self.simulation_one_hour_in_seconds = int(config.get('simulation_one_hour_in_seconds',
                                                                 self.simulation_one_hour_in_seconds))
        Timer.created_time = Timer.get_cur_time()
        Timer.simulation = self.simulation
        Timer.sim_start_time = self.simulation_start_time
        Timer.sim_one_hr_in_sec = self.simulation_one_hour_in_seconds

        # TODO: Move these into appropriate dependency class (probably ConsensusMarket):
        #  reschedule_interval = float(config.get('reschedule_interval'))
        #  self.reschedule_interval = timedelta(seconds=reschedule_interval) if reschedule_interval \
        #   else self.reschedule_interval

        # Configure TransactiveNode Component Classes
        try:
            self.meterPoints = self.configure_dependencies(config.get('meterPoints'), 'MeterPoints')
            self.informationServiceModels = self.configure_dependencies(config.get('informationServiceModels'),
                                                                        'InformationServiceModels')
            self.localAssets = self.configure_dependencies(config.get('localAssets'), 'LocalAssets')
            self.markets = self.configure_dependencies(config.get('markets'), 'Markets')
            self.neighbors = self.configure_dependencies(config.get('neighbors'), 'Neighbors')
        except ValueError as e:
            _log.error("ERROR PROCESSING CONFIGURATION {}".format(e))
            raise

        # TODO: This could probably be pushed down to the market constructor, but other places that initialize markets
        #  would need to be updated as well so it doesn't do all this twice.
        for market in self.markets:
            market.isNewestMarket = True
            market.check_intervals()
            market.check_marginal_prices(self)
            # market.marketState = MarketState.Delivery # TODO: Why is this setting the market to delivery at the start?

            delivery_start_time = market.marketClearingTime + market.deliveryLeadTime
            next_analysis_time = self.simulation_start_time if self.simulation else delivery_start_time
            _log.debug("{} schedule to run at exp_time: {} analysis_time: {}".format(self.name,
                                                                                     delivery_start_time,
                                                                                     next_analysis_time))
            for p in market.marginalPrices:
                _log.debug("Market name: {} Initial marginal prices: {}".format(market.name, p.value))
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
            cls = config.pop('class_name')
            # TODO: What will be the default directory for classes in the TransactiveNodeAgent module?
            default_module_name = 'tns.' + camel_to_snake(cls)
            module = config.pop('module_name', default_module_name)
            _log.debug(f'module_name is: {module}')
            module = importlib.import_module(module)
            config['transactive_node'] = self
            dependency = getattr(module, cls)(**config)
            dependencies.append(dependency)
        dep_names = [d.name for d in dependencies]
        if len(dep_names) != len(set(dep_names)):
            raise ValueError(f'Configured {dependency_type} have duplicate names: {[d.name for d in dependencies]}')
        return dependencies

    def state_machine_loop(self):
        # This is the entire timing logic. It relies on current market object's state machine method events()
        while not self._stop_agent:  # a condition may be added to provide stops or pauses.
            markets_to_remove = []
            for market in self.markets:
                market.events(self)
                # _log.debug("Markets: {}, Market name: {}, Market state: {}".format(len(self.markets),
                #                                                                   self.markets[i].name,
                #                                                                   self.markets[i].marketState))

                if market.marketState == MarketState.Expired:
                    markets_to_remove.append(market)
                # NOTE: A delay may be added, but the logic of the market(s) alone should be adequate to drive system
                # activities
                gevent.sleep(0.01)
            for mkt in markets_to_remove:
                _log.debug("Market name: {}, Market state: {}. It will be removed shortly".format(mkt.name,
                                                                                                  mkt.marketState))
                self.markets.remove(mkt)

    @Core.receiver('onstop')
    def onstop(self, sender, **kwargs):
        self._stop_agent = True


def main():
    try:
        utils.vip_main(TransactiveNodeAgent)
    except Exception as e:
        _log.exception(f'unhandled exception: {e}')


if __name__ == '__main__':
    # Entry point for script
    sys.exit(main())
