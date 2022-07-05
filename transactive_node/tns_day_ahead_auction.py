import sys
import tent.day_ahead_auction as day_ahead_auction
from transactive_node.tns_real_time_auction import TNSRealTimeAuction
from transactive_node.tns_auction import TNSAuction

sys.modules['tent.day_ahead_auction'].RealTimeAuction = TNSRealTimeAuction


class TNSDayAheadAuction(day_ahead_auction.DayAheadAuction, TNSAuction):
    def __init__(self, *args, **kwargs):
        super(TNSDayAheadAuction, self).__init__(*args, **kwargs)
