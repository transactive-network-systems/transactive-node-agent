import json
import logging

from volttron.platform.agent import utils
from volttron.platform.messaging import headers as headers_mod

from tent.neighbor import Neighbor
from tent.utils.helpers import format_timestamp, json_encoder
from tent.utils.timer import Timer


utils.setup_logging()
_log = logging.getLogger(__name__)


class TNSNeighbor(Neighbor):
    def __init__(self,
                 subscription_topic_postfix,
                 publication_topic_postfix,
                 *args, **kwargs):
        super(TNSNeighbor, self).__init__(*args, **kwargs)

        tn = self.tn()
        subscription_topic_postfix = str(subscription_topic_postfix)
        s_topic = f'{tn.db_topic}/{self.name}/{tn.name}'
        self.subscribeTopic = s_topic if not subscription_topic_postfix else f'{s_topic}/{subscription_topic_postfix}'
        publication_topic_postfix = str(publication_topic_postfix)
        p_topic = f'{tn.db_topic}/{tn.name}/{self.name}'
        self.publishTopic = p_topic if not publication_topic_postfix else f'{p_topic}/{publication_topic_postfix}'

        # Subscribe to neighbor publishes
        tn.vip.pubsub.subscribe(peer='pubsub',
                                prefix=self.subscribeTopic,
                                callback=self.new_transactive_signal,
                                all_platforms=tn.subscribe_all_platforms)
        _log.info(f'{tn.name} {self.name} neighbor subscribed to {self.subscribeTopic}')
        _log.debug(f'{tn.name} {self.name} neighbor getDict: {self.getDict()}')

    def new_transactive_signal(self, peer, sender, bus, topic, headers, message):
        tn = self.tn()
        _log.debug(f'At {Timer.get_cur_time()}, {tn.name}  receives new transactive signal from {self.name}'
                   f' neighbor -- peer: {peer}, sender: {sender}, bus: {bus}, topic: {topic}, headers: {headers},'
                   f' message: {message}')
        curves = message['curves']
        # TODO: These properties may not be needed anymore unless they are necessary for the consensus mkt.
        # source = message['source']
        # start_of_cycle = message['start_of_cycle']
        # fail_to_converged = message['fail_to_converged']

        self.receivedCurves = curves
        _log.debug(f'{tn.name} received new transactive signal from {self.name}: {curves}')
        # TODO: Do we need to run a callback on the TN if this is an upstairs neighbor?

    def publish_signal(self, transactive_records):
        msg = json.dumps(transactive_records, default=json_encoder)
        msg = json.loads(msg)
        tn = self.tn()
        if tn:
            if self.publishTopic:
                tn.vip.pubsub.publish(peer='pubsub',
                                      topic=self.publishTopic,
                                      message={'curves': msg,
                                               # TODO: These properties are probably not necessary unless
                                               #  needed for consensus market.
                                               # 'source': self.location,
                                               # #'start_of_cycle': start_of_cycle,
                                               # 'fail_to_converged': fail_to_converged,
                                               # 'tnt_market_name': market.name
                                               })
            topic = tn.transactive_record_topic
            headers = {headers_mod.DATE: format_timestamp(Timer.get_cur_time())}
            tn.vip.pubsub.publish(peer='pubsub', topic=topic, headers=headers, message=msg)
