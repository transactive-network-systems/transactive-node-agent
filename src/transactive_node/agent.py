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

from copy import deepcopy
from datetime import datetime
from typing import Callable

from tent.transactive_node import TransactiveNode
from tent.utils.helpers import json_encoder, require_aware
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
    def __init__(self, *args, **kwargs):
        Agent.__init__(self, *args, **kwargs)
        self._configured = False
        self._state_machine_greenlet = None
        self._subscriptions: list[tuple[str, Callable, bool]] = []  # (prefix, wrapper, all_platforms) registered
        self._init_node_state()
        self.vip.config.set_default("config", self.default_config)
        self.vip.config.subscribe(self.configure_main, actions=["NEW", "UPDATE"], pattern="config")

    def _init_node_state(self):
        """A fresh TransactiveNode (new identity and creation time, empty component lists, no data manager) with the
        agent's own defaults on top. Called at construction and again when a configuration is torn down."""
        TransactiveNode.__init__(self)
        self._stop_agent = False
        self.db_topic = 'TNS'
        self.subscribe_all_platforms = False
        self.topics: dict = {}
        self.default_config.update({
            # Agent Configurations:
            "db_topic": self.db_topic,
            "subscribe_all_platforms": self.subscribe_all_platforms,
            "topics": self.topics,
        })

    def _update_node_configurations(self, configs):
        updated_configs = super(TransactiveNodeAgent, self)._update_node_configurations(configs)

        # Agent Configurations:
        self.db_topic = updated_configs.get('db_topic', self.db_topic)
        self.subscribe_all_platforms = bool(updated_configs.get('subscribe_all_platforms', self.subscribe_all_platforms))
        self.topics: dict = {
            # Archive records from the node's VolttronDataManager, captured by the transactive-node historian.
            'archive': f'{self.db_topic}/{self.name}/archive',
        }
        # Set up transactive signaling topics:
        for neighbor in configs.get('neighbors', []):
            neighbor_name = neighbor.get('name')
            if not neighbor_name:
                _log.warning(f'No name found in neighbor configuration: {neighbor}')
            self.topics[f'{neighbor_name}_incoming_transactive_signal'] = f'{self.db_topic}/{neighbor_name}/{self.name}'
            self.topics[f'{neighbor_name}_outgoing_transactive_signal'] = f'{self.db_topic}/{self.name}/{neighbor_name}'
        self.topics.update(updated_configs.get('topics', self.topics))
        return updated_configs

    def configure_main(self, config_name, action, contents):
        """Build the node from the configuration store's `config`.

        An UPDATE is a full rebuild: the previous configuration is torn down first (loop stopped, bus subscriptions
        dropped, every component forgotten) and the node is built again as if the agent had been restarted. The rebuilt
        node is a new run: it has a new identity and archive row, and the simulation clock is re-anchored unless
        simulation_anchor_time is configured.
        """
        _log.info('Received configuration {} signal: {}'.format(action, config_name))
        if self._configured:
            _log.info('Tearing down the previous node configuration before applying this one.')
            self._teardown_node()
        self._configured = True
        # TENT's configure consumes the dicts it is given (class_name and module_name are popped out of each component
        # configuration), so work on a copy: the config store hands over the same contents again on a later UPDATE.
        self.configure_node(deepcopy(contents))
        self._state_machine_greenlet = self.core.spawn_later(5, self.state_machine_loop)

    def _teardown_node(self):
        """Undo the current configuration: stop the state-machine greenlet, unsubscribe every prefix this configuration
        registered, and reset the node to its unconfigured state (no components, no data manager, fresh identity)."""
        self._stop_agent = True
        greenlet, self._state_machine_greenlet = self._state_machine_greenlet, None
        if greenlet is not None and not greenlet.dead:
            greenlet.kill()
        for prefix, wrapper, all_platforms in self._subscriptions:
            _log.info(f'Unsubscribing from topic: {prefix}')
            self.vip.pubsub.unsubscribe(peer='pubsub', prefix=prefix, callback=wrapper, all_platforms=all_platforms)
        self._subscriptions = []
        self._init_node_state()

    def state_machine_loop(self):
        """Run market state-machine iterations until the agent stops.

        Each iteration yields to the hub at least once, so a node with no markets does not spin, and an exception in an
        iteration is logged with its traceback and the loop continues; the greenlet does not die on a single failure.
        """
        while not self._stop_agent:
            try:
                self._state_machine_iteration()
            except Exception:
                _log.exception('State machine iteration failed; continuing with the next iteration.')
            self._sleep()

    @Core.receiver('onstop')
    def onstop(self, _, **__):
        self._stop_agent = True
        self._state_machine_greenlet = None

    def publish(self, publish_key, message, date_time=None):
        date_time = date_time if date_time else Timer.now()
        # DATE is what the base historian reads for record capture; TIMESTAMP is what TENT subscriptions read.
        headers = {headers_mod.DATE: date_time.isoformat(), headers_mod.TIMESTAMP: date_time.isoformat()}
        msg = json.loads(json.dumps(message, default=json_encoder))
        publish_topic = self.topics.get(publish_key)
        if publish_topic:
            if publish_topic == 'IGNORE':
                return
            self.vip.pubsub.publish(peer='pubsub', topic=publish_topic, headers=headers, message=msg)
        else:
            _log.warning(f'Attempted to publish to unknown publish_key: {publish_key}')

    def issue_signal(self, recipient: str, signal_dict: dict):
        try:
            serialized_message = json.loads(json.dumps(signal_dict))
            self.publish(f'{recipient}_outgoing_transactive_signal', message=serialized_message)
        except (TypeError, ValueError) as e:
            _log.warning(f'Failed to serialize outgoing signal to {recipient}: {signal_dict} with error: {e}')

    def register_subscription(self, subscribe_key, callback):
        """Subscribe the callback to the topic the configuration maps `subscribe_key` to.

        The platform matches subscriptions by string prefix, so a subscription to devices/.../HP1 also receives
        devices/.../HP10; the wrapper accepts a publication only when its topic is the configured topic itself or lies
        below it (topic == prefix or topic starts with prefix + '/').
        """
        subscription_topic = self.topics.get(subscribe_key)
        if not subscription_topic:
            _log.warning(f'No subscription topic registered for subscribe_key: {subscribe_key}')
            return

        # TODO: This really needs to be aware of the topic format that is being parsed, i.e., devices, weather, etc.
        #  We could do separate wrappers for each, or have configurable transforms.
        def wrapper(_, __, ___, topic, headers, message):
            if not (topic == subscription_topic or topic.startswith(subscription_topic + '/')):
                return
            try:
                time_received = self._time_from_headers(headers)
                if topic.startswith('devices/') or topic.startswith('analysis/'):
                    message = message[0]
                # TODO: Handle datalogger & any other topic types we might encounter.
                callback(time_received, subscribe_key, message)
            except Exception:
                _log.exception(f'Subscription callback for key {subscribe_key!r} failed on topic "{topic}".')

        _log.info(f'Subscribing for key: {subscribe_key} to topic: {subscription_topic}')
        self.vip.pubsub.subscribe(peer='pubsub',
                                  prefix=subscription_topic,
                                  callback=wrapper,
                                  all_platforms=self.subscribe_all_platforms)
        self._subscriptions.append((subscription_topic, wrapper, self.subscribe_all_platforms))

    @staticmethod
    def _time_from_headers(headers) -> datetime:
        """The publication's TIMESTAMP header as an aware datetime; the node's current time when there is none."""
        timestamp = headers.get(headers_mod.TIMESTAMP) if headers else None
        if not timestamp:
            return Timer.now()
        return require_aware(datetime.fromisoformat(timestamp), f'{headers_mod.TIMESTAMP} header')

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
