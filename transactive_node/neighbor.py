import logging
import weakref
from typing import Iterable

from volttron.platform.agent import utils

from ...TNT_Version3.PyCode.neighbor_model import Neighbor
from ...TNT_Version3.PyCode.timer import Timer
from ...TNT_Version3.PyCode.vertex import Vertex
from ...TNT_Version3.PyCode.direction import Direction

utils.setup_logging()
_log = logging.getLogger(__name__)


class TNSNeighbor(Neighbor):
    def __init__(self, subscription_topic_postfix, publication_topic_postfix, *args, **kwargs):
        super(TNSNeighbor, self).__init__(*args, **kwargs)

        subscription_topic_postfix = str(subscription_topic_postfix)
        s_topic = f'{self.tn.db_topic}/{self.name}/{self.tn.name}'
        self.subscribeTopic = s_topic if not subscription_topic_postfix else f'{s_topic}/{subscription_topic_postfix}'
        publication_topic_postfix = str(publication_topic_postfix)
        p_topic = f'{self.tn.db_topic}/{self.tn.name}/{self.name}'
        self.publishTopic = p_topic if not publication_topic_postfix else f'{p_topic}/{publication_topic_postfix}'

        # Subscribe to neighbor publishes
        self.tn.vip.pubsub.subscribe(peer='pubsub',
                                     prefix=self.subscribeTopic,
                                     callback=self.new_transactive_signal,
                                     all_platforms=self.tn.subscribe_all_platforms)
        _log.info(f'{self.tn.name} {self.name} neighbor subscribed to {self.subscribeTopic}')
        _log.debug(f'{self.tn.name} {self.name} neighbor getDict: {self.getDict()}')

    def new_transactive_signal(self, peer, sender, bus, topic, headers, message):
        _log.debug('At {}, {}  receives new transactive signal: {}'.format(Timer.get_cur_time(),
                                                                           self.name, message))
        source = message['source']
        curves = message['curves']
        start_of_cycle = message['start_of_cycle']
        fail_to_converged = message['fail_to_converged']

        self.receivedCurves = curves
        _log.debug(f'{self.tn.name} received new transactive signal from {self.name}:')
        # TODO: Do we need to run a callback on the TN if this is an upstairs neighbor?
