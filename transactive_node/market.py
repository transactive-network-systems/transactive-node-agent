from datetime import timedelta

from ...TNT_Version3.PyCode.market import Market
from ...TNT_Version3.PyCode.market_state import MarketState
from ...TNT_Version3.PyCode.market_types import MarketTypes
from ...TNT_Version3.PyCode.method import Method


class TNSMarket(Market):
    def __init__(self):
        super(TNSMarket, self).__init__()

    def configure(self, transactive_node, config):
        # [timedelta] Time in market state "Active"
        self.activationLeadTime = timedelta(seconds=config.get('activation_lead_time', self.activationLeadTime.seconds))
        commitment = config.get('commitment', self.commitment)
        if not isinstance(commitment, bool) or isinstance(commitment, int) or isinstance(commitment, float):
            raise ValueError(f'Configured parameter "commitment" must be bool, not {type(commitment)}')
        self.commitment = bool(commitment)
        self.defaultPrice = float(config.get('default_price', self.defaultPrice))
        self.deliveryLeadTime = timedelta(seconds=config.get('delivery_lead_time', self.deliveryLeadTime.seconds))
        self.dualityGapThreshold = float(config.get('duality_gap_threshold', self.dualityGapThreshold))
        self.futureHorizon = timedelta(seconds=config.get('future_horizon', self.futureHorizon.seconds))
        self.initialMarketState = MarketState[config.get('initial_market_state', self.marketState.name)]
        self.intervalDuration = timedelta(seconds=config.get('interval_duration', self.intervalDuration.seconds))
        self.intervalsToClear = int(config.get('intervals_to_clear', self.intervalsToClear))
        self.marketClearingInterval = timedelta(seconds=config.get('market_clearing_interval',
                                                                   self.marketClearingInterval.seconds))
        self.marketClearingTime = market_clearing_time  # TODO: [datetime] Time that a market object clears
        self.marketLeadTime = timedelta(seconds=config.get('market_lead_time', self.marketLeadTime.seconds))
        self.marketOrder = int(config.get('market_order', self.marketOrder))
        self.marketSeriesName = str(config.get('market_series_name', self.marketSeriesName))
        self.marketToBeRefined = market_to_be_refined  # TODO: [Market] Pointer to market to be refined or corrected
        self.marketType = MarketTypes[config.get('market_type', self.marketType.name)]
        self.method = Method[config.get('method', self.method.name)]
        self.name = str(config.get('name', self.name))
        self.negotiationLeadTime = timedelta(seconds=config.get('negotiation_lead_time',
                                                                self.negotiationLeadTime.seconds))
        self.nextMarketClearingTime = next_market_clearing_time  # TODO: [datetime] Time of next market object's clearing
        self.priorMarketInSeries = prior_market_in_series  # TODO [Market] Pointer to preceding market in this market series