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
import os
import sys
import time

from copy import deepcopy
from datetime import datetime
from typing import Callable
from uuid import UUID

from tent.transactive_node import TransactiveNode
from tent.utils.helpers import json_dumps, require_aware
from tent.utils.timer import Timer
from volttron_archivable import VolttronDataManager

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
    SIMULATION_KEYS = ('simulation', 'simulation_start_time', 'simulation_one_hour_in_seconds', 'simulation_anchor_time')
    SIGNAL_KEY_SUFFIX = '_incoming_transactive_signal'
    SENDER_KEY = 'sender'                 # agent key of a neighbor entry: the identity its signals must arrive from
    SENDER_MISMATCH_LOG_PERIOD = 60.0     # [s] one warning per presented sender per period, with the count suppressed
    ANCHOR_ENVIRONMENT_VARIABLE = 'TENT_SIMULATION_ANCHOR_TIME'

    def __init__(self, *args, **kwargs):
        Agent.__init__(self, *args, **kwargs)
        self._configured = False
        self._simulation_settings: dict | None = None  # the configured simulation block of the running configuration
        self._state_machine_greenlet = None
        self._subscriptions: list[tuple[str, Callable, bool]] = []  # (prefix, wrapper, all_platforms) registered
        self._init_node_state()
        self.vip.config.set_default("config", self.default_config)
        self.vip.config.subscribe(self.configure_main, actions=["NEW", "UPDATE"], pattern="config")

    def _init_node_state(self, node_id: UUID | None = None, created: datetime | None = None):
        """A fresh TransactiveNode (empty component lists, no data manager) with the agent's own defaults on top. The
        node is a new one unless `node_id` and `created` hand it the identity of the one being replaced. Called at
        construction and again when a configuration is torn down."""
        TransactiveNode.__init__(self, node_id=node_id, created=created)
        self._stop_agent = False
        self.db_topic = 'TNS'
        self.subscribe_all_platforms = False
        self.topics: dict = {}
        self._neighbor_senders: dict[str, str] = {}      # neighbor name -> identity its signals must be presented from
        self._sender_forms_logged: set[str] = set()      # neighbors whose accepted sender form was logged
        self._sender_mismatches: dict[str, dict] = {}    # presented sender -> {'since', 'count'} of dropped signals
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
        archive_root = f'{self.db_topic}/archive/{self.name}'
        self.topics: dict = {
            # The node's archive traffic with the Application Historian (see VolttronDataManager): records out,
            # table definitions out (pubsub registration), the historian's registration status in.
            VolttronDataManager.ARCHIVE_KEY: f'{archive_root}/records',
            VolttronDataManager.DEFINITIONS_KEY: f'{archive_root}/definitions',
            VolttronDataManager.STATUS_KEY: f'{archive_root}/status',
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

        The contents are validated through TENT's configuration models first, so a configuration the models reject is
        refused before the running node is touched. On an UPDATE the running node is asked to take the new
        configuration in place (TransactiveNode.reconfigure: only reconfigurable fields of existing components
        changed); when it cannot, the node is rebuilt: the previous configuration is torn down (loop stopped, bus
        subscriptions dropped, every component forgotten) and the node is built again from the new contents. When the
        simulation settings are unchanged the rebuilt node continues the same run: it keeps its identity and creation
        time, so its archive row is appended as a new version and the simulated clock keeps its anchor. When they
        changed, or on a NEW, the node is a new run.
        """
        _log.info('Received configuration {} signal: {}'.format(action, config_name))
        # Work on a copy: the config store hands over the same object again on a later UPDATE, and the anchor inserted
        # below must not leak into it.
        contents = deepcopy(contents)
        self._apply_environment_anchor(contents)
        # The neighbors' sender bindings are the agent's (the transport's), not TENT's: taken out before TENT sees the
        # configuration, applied by the subscription wrapper.
        neighbor_senders = self._extract_neighbor_senders(contents)
        TransactiveNode.validate_configuration(contents)  # raises (ConfigurationError) before anything is torn down
        simulation_settings = {key: contents.get(key) for key in self.SIMULATION_KEYS}
        identity = {}
        if self._configured:
            if action == 'UPDATE' and self.reconfigure(contents):
                self._neighbor_senders = neighbor_senders
                _log.info(f'Node {self.name!r} took the updated configuration in place; no rebuild.')
                return
            if action == 'UPDATE' and simulation_settings == self._simulation_settings:
                identity = {'node_id': self.id, 'created': self.real_time_created}
                _log.info(f'Rebuilding node {self.name!r} for the updated configuration as the same run '
                          f'(id {self.id}): the simulation settings are unchanged.')
            elif action == 'UPDATE':
                _log.info(f'Rebuilding node {self.name!r} for the updated configuration as a new run: '
                          f'the simulation settings changed.')
            else:
                _log.info(f'Rebuilding node {self.name!r} as a new run: a {action} configuration is always a new run.')
            self._teardown_node(**identity)
        self._configured = True
        self._simulation_settings = simulation_settings
        self._neighbor_senders = neighbor_senders  # before configure_node: neighbors subscribe as they are built
        self.configure_node(contents)
        self._state_machine_greenlet = self.core.spawn_later(5, self.state_machine_loop)

    def _extract_neighbor_senders(self, contents: dict) -> dict[str, str]:
        """Take the `sender` key out of every neighbor entry (TENT's Neighbor.Config does not know it) and return the
        bindings, neighbor name -> expected sender. A transactive neighbor without one is accepted from any sender,
        which is said once here at WARNING."""
        senders = {}
        for entry in contents.get('neighbors') or []:
            if not isinstance(entry, dict):
                continue
            name = entry.get('name')
            sender = entry.pop(self.SENDER_KEY, None)
            if sender is not None:
                if not isinstance(sender, str) or not sender:
                    raise ValueError(f'neighbor {name!r}: sender must be a non-empty string, the identity its signals '
                                     f'arrive from ("identity@instance" or "identity"); got {sender!r}.')
                senders[name] = sender
            elif entry.get('transactive'):
                _log.warning(f'Signals from {name!r} are not bound to a sender: any publisher on the bus may send them. '
                             f'Set "sender" in the neighbor entry.')
        return senders

    # ------------------------------------------------------------------------------------------- sender identity

    def _neighbor_of_signal_key(self, subscribe_key: str) -> str | None:
        """The neighbor a transactive-signal subscribe key belongs to; None for any other key."""
        if subscribe_key.endswith(self.SIGNAL_KEY_SUFFIX):
            return subscribe_key[:-len(self.SIGNAL_KEY_SUFFIX)]
        return None

    def _sender_accepted(self, neighbor: str, presented: str, topic: str) -> bool:
        """Whether a signal claiming to come from `neighbor` may be taken from the publisher the platform presented.

        The binding is `identity@instance`, or a bare identity meaning any platform. A presented identity equal to the
        binding is accepted; so is the bare tail of a qualified binding while the router presents bare identities (which
        form arrived is logged once per neighbor at info). Anything else is dropped and reported (rate-limited).
        """
        expected = self._neighbor_senders.get(neighbor)
        if expected is None:
            return True
        bare_expected = expected.split('@', 1)[0]
        if presented == expected or ('@' in expected and presented == bare_expected):
            if neighbor not in self._sender_forms_logged:
                self._sender_forms_logged.add(neighbor)
                form = 'qualified' if presented == expected and '@' in expected else 'bare'
                _log.info(f'Signals from {neighbor!r} arrive from {presented!r} (the {form} form of the binding '
                          f'{expected!r}).')
            return True
        self._report_sender_mismatch(neighbor, expected, presented, topic)
        return False

    def _report_sender_mismatch(self, neighbor: str, expected: str, presented: str, topic: str):
        """One WARNING per presented sender per SENDER_MISMATCH_LOG_PERIOD, carrying the number suppressed since."""
        now = time.monotonic()
        window = self._sender_mismatches.get(presented)
        if window is not None and now - window['since'] < self.SENDER_MISMATCH_LOG_PERIOD:
            window['count'] += 1
            return
        suppressed = window['count'] if window else 0
        more = f' ({suppressed} more from this sender dropped in the last {self.SENDER_MISMATCH_LOG_PERIOD:.0f} s)' \
            if suppressed else ''
        _log.warning(f'Dropped a transactive signal on "{topic}": it claims to come from {neighbor!r}, whose sender is '
                     f'{expected!r}, but the platform presented {presented!r}.{more}')
        self._sender_mismatches[presented] = {'since': now, 'count': 0}

    def _apply_environment_anchor(self, contents: dict):
        """Insert the shared simulation anchor from the environment when the configuration does not set one. Containers
        cannot write a per-run anchor into their mounted configurations; the compose file sets it once for every node."""
        if contents.get('simulation_anchor_time'):
            return
        anchor = os.environ.get(self.ANCHOR_ENVIRONMENT_VARIABLE)
        if anchor:
            contents['simulation_anchor_time'] = anchor
            _log.info(f'simulation_anchor_time {anchor!r} taken from the environment ({self.ANCHOR_ENVIRONMENT_VARIABLE}).')

    def _teardown_node(self, node_id: UUID | None = None, created: datetime | None = None):
        """Undo the current configuration: stop the state-machine greenlet, unsubscribe every prefix this configuration
        registered, and reset the node to its unconfigured state (no components, no data manager). The reset node is
        new unless `node_id` and `created` carry the identity over."""
        self._stop_agent = True
        greenlet, self._state_machine_greenlet = self._state_machine_greenlet, None
        if greenlet is not None and not greenlet.dead:
            greenlet.kill()
        for prefix, wrapper, all_platforms in self._subscriptions:
            _log.info(f'Unsubscribing from topic: {prefix}')
            self.vip.pubsub.unsubscribe(peer='pubsub', prefix=prefix, callback=wrapper, all_platforms=all_platforms)
        self._subscriptions = []
        # The data manager publishes queued archive records on a timer; stop it so the queue is flushed and no greenlet
        # outlives the configuration (every DataManager has stop(); the base one holds nothing).
        if self._data_manager is not None:
            self._data_manager.stop()
        self._init_node_state(node_id=node_id, created=created)

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
        """Publish `message` on the topic mapped to `publish_key`. The message is serialised with TENT's encoder and
        standard JSON only: a NaN or infinite number raises ValueError here, so nothing non-JSON leaves the agent."""
        date_time = date_time if date_time else Timer.now()
        # DATE is what the base historian reads for record capture; TIMESTAMP is what TENT subscriptions read.
        headers = {headers_mod.DATE: date_time.isoformat(), headers_mod.TIMESTAMP: date_time.isoformat()}
        msg = json.loads(json_dumps(message))
        publish_topic = self.topics.get(publish_key)
        if publish_topic:
            if publish_topic == 'IGNORE':
                return
            self.vip.pubsub.publish(peer='pubsub', topic=publish_topic, headers=headers, message=msg)
        else:
            _log.warning(f'Attempted to publish to unknown publish_key: {publish_key}')

    def issue_signal(self, recipient: str, signal_dict: dict):
        try:
            self.publish(f'{recipient}_outgoing_transactive_signal', message=signal_dict)
        except (TypeError, ValueError):
            _log.exception(f'Outgoing signal to {recipient} cannot be serialised and was not sent: {signal_dict}')

    def register_subscription(self, subscribe_key, callback, all_platforms: bool | None = None):
        """Subscribe the callback to the topic the configuration maps `subscribe_key` to.

        The platform matches subscriptions by string prefix, so a subscription to devices/.../HP1 also receives
        devices/.../HP10; the wrapper accepts a publication only when its topic is the configured topic itself or lies
        below it (topic == prefix or topic starts with prefix + '/'). `all_platforms` overrides the node's
        subscribe_all_platforms setting for this subscription (the historian's status topic is subscribed everywhere).
        """
        all_platforms = self.subscribe_all_platforms if all_platforms is None else bool(all_platforms)
        subscription_topic = self.topics.get(subscribe_key)
        if not subscription_topic:
            _log.warning(f'No subscription topic registered for subscribe_key: {subscribe_key}')
            return

        # TODO: This really needs to be aware of the topic format that is being parsed, i.e., devices, weather, etc.
        #  We could do separate wrappers for each, or have configurable transforms.
        neighbor = self._neighbor_of_signal_key(subscribe_key)

        def wrapper(_, sender, __, topic, headers, message):
            if not (topic == subscription_topic or topic.startswith(subscription_topic + '/')):
                return
            if neighbor is not None and not self._sender_accepted(neighbor, sender, topic):
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
                                  all_platforms=all_platforms)
        self._subscriptions.append((subscription_topic, wrapper, all_platforms))

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
