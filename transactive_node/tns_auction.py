from tent.auction import Auction
from tent.helpers import format_timestamp
from tent.timer import Timer

from volttron.platform.messaging import headers as headers_mod


class TNSAuction(Auction):
    def __init__(self, *args, **kwargs):
        super(TNSAuction, self).__init__(*args, **kwargs)

    def transition_from_active_to_negotiation(self, my_transactive_node):
        super(TNSAuction, self).transition_from_active_to_negotiation(my_transactive_node)
        self.publish_records(my_transactive_node)
        # Publish local asset info
        # SN: No need, publish transactive operation instead
        '''topic = "{}/{}".format(my_transactive_node.local_asset_topic,
                               my_transactive_node.localAssets[x].name)
        msg = local_asset.getDict()
        headers = {headers_mod.DATE: format_timestamp(Timer.get_cur_time())}
        _log.debug(
            "AUCTION:transition_from_active_to_negotiation: {} and info: {}".format(topic, msg))
        my_transactive_node.vip.pubsub.publish("pubsub",topic, headers, msg)
        '''

    def while_in_negotiation(self, my_transactive_node):
        super(TNSAuction, self).while_in_negotiation(my_transactive_node)

        headers = {headers_mod.DATE: format_timestamp(Timer.get_cur_time())}
        for local_asset in my_transactive_node.localAssets:
            # Publish local asset info
            topic = "{}/{}".format(my_transactive_node.local_asset_topic,
                                   local_asset.name)
            msg = local_asset.getDict()
            headers = {headers_mod.DATE: format_timestamp(Timer.get_cur_time())}
            # _log.debug(
            #    "AUCTION:while_in_negotiation: {} and info: {}".format(topic, msg))
            # my_transactive_node.vip.pubsub.publish("pubsub", topic, headers, msg)

    def transition_from_inactive_to_active(self, my_transactive_node):
        super(TNSAuction, self).transition_from_inactive_to_active(my_transactive_node)
        self.publish_records(my_transactive_node)

    def transition_from_negotiation_to_market_lead(self, my_transactive_node):
        super(TNSAuction, self).transition_from_negotiation_to_market_lead(my_transactive_node)
        self.publish_records(my_transactive_node)

    def transition_from_market_lead_to_delivery_lead(self, my_transactive_node):
        super(TNSAuction, self).transition_from_market_lead_to_delivery_lead(my_transactive_node)
        self.publish_records(my_transactive_node)

    def transition_from_delivery_lead_to_delivery(self, my_transactive_node):
        super(TNSAuction, self).transition_from_delivery_lead_to_delivery(my_transactive_node)
        headers = {headers_mod.DATE: format_timestamp(Timer.get_cur_time())}
        msg = dict()
        msg['tnt_market_name'] = self.name
        my_transactive_node.vip.pubsub.publish(peer='pubsub',
                                               topic=my_transactive_node.market_balanced_price_topic,
                                               headers=headers,
                                               message=msg)
        self.publish_records(my_transactive_node)

    def transition_from_reconcile_to_expired(self, my_transactive_node):
        super(TNSAuction, self).transition_from_reconcile_to_expired(my_transactive_node)
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

        topic = "{}/{}".format(my_transactive_node.transactive_operation_topic, self.name)
        my_transactive_node.vip.pubsub.publish(peer='pubsub', topic=topic,
                                               headers=headers, message=transactive_operation)
#        _log.debug("AUCTION: Publishing on market topic: {} and info: {}".format(topic, transactive_operation))
