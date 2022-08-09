import sys

import tent.market.day_ahead_auction as day_ahead_auction

from tns_real_time_auction import TNSRealTimeAuction
from tns_auction import TNSAuction

sys.modules['tent.day_ahead_auction'].RealTimeAuction = TNSRealTimeAuction


class TNSDayAheadAuction(day_ahead_auction.DayAheadAuction, TNSAuction):
    def __init__(self, *args, **kwargs):
        super(TNSDayAheadAuction, self).__init__(*args, **kwargs)
