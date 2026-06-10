from collections import defaultdict

from tent.market.auction import Auction
from tent.utils.timer import Timer

# noinspection PyUnresolvedReferences
from volttron.platform.messaging import headers as headers_mod

def balanced_price(self: Auction):
    """This was used in RealTimeMarket.transition_from_delivery_lead_to_delivery(), after the call to super.
        It may be useful for future reference.

        The default configuration for balanced_price_record was:
            'balanced_price_record' = f'{self.db_topic}/{self.name}/market_balanced_prices',
    """
    headers = {headers_mod.DATE: Timer.now().isoformat()}
    scheduled_powers = defaultdict(dict)
    for entity in self.tn.neighbors + self.tn.local_assets:
        for p in entity.scheduled_powers:
            if p.interval.market is self:
                scheduled_powers[p.interval.start_time.isoformat()][entity.name] = p.value
    msg = {
        'tnt_market_name': self.name,
        'balanced_prices': {p.interval.start_time.isoformat(): p.value for p in self.marginal_prices},
        'schedule_powers': scheduled_powers
    }
    self.tn.vip.pubsub.publish(peer='pubsub',
                                           topic=self.tn.market_balanced_price_topic,
                                           headers=headers,
                                           message=msg)
    self.publish_records(self.tn)


def transactive_operation_real_time(self: Auction):
    """This was used in RealTimeMarket.publish_records().
        It may be useful for future reference.
        
        publish_records() was called from the TNSAuction class 
        (parent of RealTimeTNSAuction and DayAheadTNSAuction) in the following methods:
        - transition_from_negotiation_to_market_lead
        - transition_from_market_lead_to_delivery_lead
        - transition_from_delivery_lead_to_delivery
        - transition_from_reconcile_to_expired
        
        The default configuration for transactive_operation_topic was:
            'transactive_operation_record' = f'{self.db_topic}/{self.name}/transactive_operation',
    """
    headers = {headers_mod.DATE: Timer.now().isoformat()}
    transactive_operation = dict()
    transactive_operation['prices'] = list()
    transactive_operation['demand'] = dict()
    transactive_operation['demand']['bid'] = dict()

    for idx, p in enumerate(self.marginal_prices):
        transactive_operation['prices'].append((p.interval.start_time.isoformat(), p.value))

    for neighbor in self.tn.neighbors:
        transactive_operation['demand']['bid'][neighbor.name] = neighbor.get_dict()['sent_signal']

    transactive_operation['demand']['actual'] = dict()
    transactive_operation['demand']['actual']['neighbor'] = dict()
    transactive_operation['demand']['actual']['assets'] = dict()

    for neighbor in self.tn.neighbors:
        transactive_operation['demand']['actual']['neighbor'][neighbor.name] = \
                neighbor.get_dict()['received_signal']
        for asset in self.tn.local_assets:
            transactive_operation['demand']['actual']['assets'][asset.name] = asset.get_dict()['vertices']

        topic = "{}/{}".format(self.tn.transactive_operation_topic, self.series.name)
        self.tn.vip.pubsub.publish(peer='pubsub', topic=topic,
                                               headers=headers, message=transactive_operation)


def get_dict(self): # From LocalAsset
    scheduled_powers = [(k.start_time.isoformat(), v.value) for k, v in self.scheduled_powers.all]
    vertices = [(x.interval.start_time.isoformat(), x.value.price, x.value.quantity) for x in
                self.active_vertices]
    local_asset_dict = {
        "name": self.name,
        "scheduled_power": scheduled_powers,
        "vertices": vertices
    }
    return local_asset_dict


def transactive_operation_day_ahead(self, upstream_agents=None, downstream_agents=None):
    """ This was used in TNSAuction.publish_records()
        and would have been inherited by TNSDayAheadAuction.
          It may be useful for future reference.

        publish_records() was called from the TNSAuction class
        (parent of RealTimeTNSAuction and DayAheadTNSAuction) in the following methods:
        - transition_from_negotiation_to_market_lead
        - transition_from_market_lead_to_delivery_lead
        - transition_from_delivery_lead_to_delivery
        - transition_from_reconcile_to_expired

        The default configuration for transactive_operation_topic was:
            'transactive_operation_record' = f'{self.db_topic}/{self.name}/transactive_operation',
    """
    headers = {headers_mod.DATE: Timer.now().isoformat()}
    transactive_operation = dict()
    transactive_operation['prices'] = list()
    transactive_operation['demand'] = dict()
    transactive_operation['demand']['bid'] = dict()

    for idx, p in enumerate(self.marginal_prices):
        transactive_operation['prices'].append((p.interval.start_time.isoformat(), p.value))

    for neighbor in self.tn.neighbors:
        transactive_operation['demand']['bid'][neighbor.name] = neighbor.get_dict()['sent_signal']

    topic = "{}/{}".format(self.tn.topics['transactive_operation_record'], self.series.name)
    self.tn.vip.pubsub.publish(peer='pubsub', topic=topic,
                                           headers=headers, message=transactive_operation)

