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
import json
import logging
import sys

from tent.transactive_node import TransactiveNode
from tent.utils.helpers import json_encoder
from tent.utils.timer import Timer

from importlib.metadata import distribution, PackageNotFoundError
try:
    distribution('volttron-core')
    from volttron.client.logs import setup_logging
    from volttron.client.messaging import headers as headers_mod
    from volttron.client.vip.agent import Agent, Core
    from volttron.utils import vip_main
except PackageNotFoundError:
    from volttron.platform.agent.utils import setup_logging, vip_main
    from volttron.platform.messaging import headers as headers_mod
    from volttron.platform.vip.agent import Agent, Core

setup_logging()
_log = logging.getLogger(__name__)
__version__ = '0.1'


class TransactiveNodeAgent(Agent, TransactiveNode):
    """Transactive Node Agent.

    This agent acts as a single node in a transactive network.
    It wraps the TNS.myTransactiveNode class.
    The Transactive Node Agent handles configuration of the node and its services,
    provides access to messages on the VOLTTRON message bus, and runs the loop.
    """
    def __init__(self, _, *args, **kwargs):
        _log.debug('in init')
        Agent.__init__(self, *args, **kwargs)
        _log.debug('Agent initialized')
        TransactiveNode.__init__(self)
        _log.debug('Node initialized')

        # Initial State:
        self._stop_agent = False

        # Default configuration.
        self.db_topic = 'TNS'
        self.subscribe_all_platforms = False
        self.topics: dict = {}
        self.plots_active = False  # TODO: Do the plots actually belong in the agent code, if not where?

        # Set up config store.
        self.default_config.update({
            # Agent Configurations:
            "db_topic": self.db_topic,
            "subscribe_all_platforms": self.subscribe_all_platforms,
            "topics": self.topics,
            "plots_active": self.plots_active
        })
        self.vip.config.set_default("config", self.default_config)
        self.vip.config.subscribe(self.configure_main, actions=["NEW", "UPDATE"], pattern="config")

    def _update_node_configurations(self, configs):
        updated_configs = super(TransactiveNodeAgent, self)._update_node_configurations(configs)

        # Agent Configurations:
        self.db_topic = updated_configs.get('db_topic', self.db_topic)
        self.subscribe_all_platforms = bool(updated_configs.get('subscribe_all_platforms', self.subscribe_all_platforms))
        self.topics:dict = {
            'balanced_price_record': f"{self.db_topic}/{self.name}/market_balanced_prices",
            'local_asset_record': f"{self.db_topic}/{self.name}/local_assets",
            'negotiation_record': f"{self.db_topic}/{self.name}/negotiation_record",
            'transactive_operation_record': f"{self.db_topic}/{self.name}/transactive_operation",
            'transactive_record': f'{self.db_topic}/{self.name}/transactive_record'
        }
        # Set up transactive signaling topics:
        for neighbor in configs.get('neighbors', []):
            neighbor_name = neighbor.get('name')
            if not neighbor_name:
                _log.warning(f'No name found in neighbor configuration: {neighbor}')
            self.topics[f'{neighbor_name}_incoming_transactive_signal'] = f'{self.db_topic}/{neighbor_name}/{self.name}'
            self.topics[f'{neighbor_name}_outgoing_transactive_signal'] = f'{self.db_topic}/{self.name}/{neighbor_name}'
        self.topics.update(updated_configs.get('topics', self.topics))
        self.plots_active = bool(updated_configs.get('plots_active', self.plots_active))
        return updated_configs

    def configure_main(self, config_name, action, contents):
        _log.info('Received configuration {} signal: {}'.format(action, config_name))
        self.vip.pubsub.unsubscribe("pubsub", None, None)
        self.configure_node(contents)
        # # TODO: This could probably be pushed down to the market constructor, but other places that initialize markets
        # #  would need to be updated as well so it doesn't do all this twice.
        # for market in self.markets:
        #     # market.marketState = MarketState.Delivery # TODO: Why is this setting the market to delivery at the start?
        #
        #     next_analysis_time = self.simulation_start_time if self.simulation else market.transition_times.delivery
        #     _log.debug("{} schedule to run at exp_time: {} analysis_time: {}".format(self.name,
        #                                                                              market.transition_times.delivery,
        #                                                                              next_analysis_time))
        #     for p in market.marginal_prices:
        #         _log.debug(f"Market: {market.name} has initial marginal prices {p.value}"
        #                    f" for interval: {p.interval.start_time}")
        self.core.spawn_later(5, self.state_machine_loop)

    def state_machine_loop(self):
        while not self._stop_agent:
            self._state_machine_iteration()

    @Core.receiver('onstop')
    def onstop(self, sender, **kwargs):
        self._stop_agent = True

    def publish(self, publish_key, message, date_time=None):
        date_time = date_time if date_time else Timer.now()
        headers = {headers_mod.DATE: date_time.isoformat()}
        msg = json.loads(json.dumps(message, default=json_encoder))
        publish_topic = self.topics.get(publish_key)
        _log.debug(f"IN TNS PUBLISH, OUTGOING TOPIC FOR PUBLISH_KEY: {publish_key} IS: {publish_topic}")
        if publish_topic:
            if publish_topic == 'IGNORE':
                return
            self.vip.pubsub.publish(peer='pubsub', topic=publish_topic, headers=headers, message=msg)
        else:
            _log.warning(f'Attempted to publish to unknown publish_key: {publish_key}')

    def issue_signal(self, recipient: str, signal_dict: dict):
        self.publish(f'{recipient}_outgoing_transactive_signal', message=serialized_signal)

    def register_subscription(self, subscribe_key, callback):
        def wrapper(_, __, ___, topic, ____, message):
            _log.debug(f'At {Timer.now()}, {self.name}  received new message on topic: {topic}')
            return callback(message)

        subscription_topic = self.topics.get(subscribe_key)
        if subscription_topic:
            self.vip.pubsub.subscribe(peer='pubsub',
                                    prefix=subscription_topic,
                                    callback=wrapper,
                                    all_platforms=self.subscribe_all_platforms)
        else:
            _log.warning(f'No subscription topic registered for subscribe_key: {subscribe_key}')

    def _sleep(self):
        gevent.sleep(self.loop_sleep_duration)

def main():
    try:
        vip_main(TransactiveNodeAgent)
    except Exception as e:
        _log.exception(f'unhandled exception: {e}')


if __name__ == '__main__':
    # Entry point for script
    sys.exit(main())
