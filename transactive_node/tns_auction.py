from collections import defaultdict

from tent.market.auction import Auction
from tent.utils.timer import Timer

from volttron.platform.messaging import headers as headers_mod


class TNSAuction(Auction):
    def __init__(self, *args, **kwargs):
        super(TNSAuction, self).__init__(*args, **kwargs)

    def transition_from_active_to_negotiation(self, my_transactive_node):
        super(TNSAuction, self).transition_from_active_to_negotiation(my_transactive_node)
        # self.publish_records(my_transactive_node)

    def while_in_negotiation(self, my_transactive_node):
        super(TNSAuction, self).while_in_negotiation(my_transactive_node)

    def transition_from_negotiation_to_market_lead(self, my_transactive_node):
        super(TNSAuction, self).transition_from_negotiation_to_market_lead(my_transactive_node)
        self.publish_records(my_transactive_node)

    def transition_from_market_lead_to_delivery_lead(self, my_transactive_node):
        super(TNSAuction, self).transition_from_market_lead_to_delivery_lead(my_transactive_node)
        self.publish_records(my_transactive_node)

    def transition_from_delivery_lead_to_delivery(self, my_transactive_node):
        super(TNSAuction, self).transition_from_delivery_lead_to_delivery(my_transactive_node)
        self.publish_records(my_transactive_node)

    def transition_from_reconcile_to_expired(self, my_transactive_node):
        super(TNSAuction, self).transition_from_reconcile_to_expired(my_transactive_node)
        self.publish_records(my_transactive_node)

    def publish_records(self, my_transactive_node, upstream_agents=None, downstream_agents=None):
        headers = {headers_mod.DATE: Timer.now().isoformat()}
        transactive_operation = dict()
        transactive_operation['prices'] = list()
        transactive_operation['demand'] = dict()
        transactive_operation['demand']['bid'] = dict()

        #        _log.debug("AUCTION: BEFORE: info: {}".format(transactive_operation))
        for idx, p in enumerate(self.marginal_prices):
            transactive_operation['prices'].append((p.interval.start_time.isoformat(), p.value))

        for neighbor in my_transactive_node.neighbors:
            transactive_operation['demand']['bid'][neighbor.name] = neighbor.get_dict()['sent_signal']

        topic = "{}/{}".format(my_transactive_node.topics['transactive_operation_record'], self.series.name)
        my_transactive_node.vip.pubsub.publish(peer='pubsub', topic=topic,
                                               headers=headers, message=transactive_operation)
#        _log.debug("AUCTION: Publishing on market topic: {} and info: {}".format(topic, transactive_operation))
