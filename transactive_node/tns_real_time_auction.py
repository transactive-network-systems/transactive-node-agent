from tent.helpers import format_timestamp
from tent.real_time_auction import RealTimeAuction
from tent.timer import Timer

from transactive_node.tns_auction import TNSAuction

from volttron.platform.messaging import headers as headers_mod


class TNSRealTimeAuction(RealTimeAuction, TNSAuction):
    def __init__(self, *args, **kwargs):
        super(TNSRealTimeAuction, self).__init__(*args, **kwargs)

    def transition_from_delivery_lead_to_delivery(self, my_transactive_node):
        super(TNSRealTimeAuction, self).transition_from_delivery_lead_to_delivery(my_transactive_node)
        headers = {headers_mod.DATE: format_timestamp(Timer.get_cur_time())}
        msg = dict()
        msg['tnt_market_name'] = self.name
        my_transactive_node.vip.pubsub.publish(peer='pubsub',
                                               topic=my_transactive_node.market_balanced_price_topic,
                                               headers=headers,
                                               message=msg)
        self.publish_records(my_transactive_node)

    def publish_records(self, my_transactive_node, upstream_agents=None, downstream_agents=None):
        headers = {headers_mod.DATE: format_timestamp(Timer.get_cur_time())}
        transactive_operation = dict()
        transactive_operation['prices'] = list()
        transactive_operation['demand'] = dict()
        transactive_operation['demand']['bid'] = dict()

        #        _log.debug("AUCTION: BEFORE: info: {}".format(transactive_operation))
        for idx, p in enumerate(self.marginalPrices):
            transactive_operation['prices'].append((format_timestamp(p.timeInterval.startTime), p.value))

        for neighbor in my_transactive_node.neighbors:
            transactive_operation['demand']['bid'][neighbor.name] = neighbor.getDict()['sent_signal']

        transactive_operation['demand']['actual'] = dict()
        transactive_operation['demand']['actual']['neighbor'] = dict()
        transactive_operation['demand']['actual']['assets'] = dict()

        for neighbor in my_transactive_node.neighbors:
            transactive_operation['demand']['actual']['neighbor'][neighbor.name] = \
                neighbor.getDict()['received_signal']
        for asset in my_transactive_node.localAssets:
            transactive_operation['demand']['actual']['assets'][asset.name] = asset.getDict()['vertices']

        topic = "{}/{}".format(my_transactive_node.transactive_operation_topic, self.name)
        my_transactive_node.vip.pubsub.publish(peer='pubsub', topic=topic,
                                               headers=headers, message=transactive_operation)
#        _log.debug("AUCTION: Publishing on market topic: {} and info: {}".format(topic, transactive_operation))
