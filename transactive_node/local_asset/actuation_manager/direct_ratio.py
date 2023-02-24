from transactive_node.local_asset.actuation_manager import ActuationManager


class DirectRatioActuationManager(ActuationManager):
    def __init__(self, **kwargs):
        super(DirectRatioActuationManager, self).__init__(**kwargs)

    def actuate(self, mkt):
        super(DirectRatioActuationManager, self).actuate(mkt)
        if self.actuation_active:
            start_time = mkt.marketClearingTime + mkt.deliveryLeadTime
            price = [p.value for p in mkt.marginalPrices if p.timeInterval.startTime == start_time][0]
            vertex_prices = [av.value.marginalPrice for av in self.parent.activeVertices
                             if av.timeInterval.startTime == start_time]
            min_price = min(vertex_prices)
            max_price = max(vertex_prices)
            price = min(max(price, min_price), max_price)  # clamp price within bid range.
            cleared_price_ratio = (price - min_price) / (max_price - min_price)
            for model in self.parent.models.values():  # TODO: Generalize this away from models in ModelFrameAsset.
                if model.actuation_topic:
                    min_set_point, max_set_point = model.set_point_range(start_time)
                    # For forward-acting (cooling) mode:
                    new_set_point = (cleared_price_ratio * (max_set_point - min_set_point)) + min_set_point
                    # TODO: For heating mode instead do commented code: Also add context property in thermostat
                    #  & lighting to determine forward or reverse acting. For thermostat, this will be two set points
                    #  and a deadband. For lighting, it is always reverse-acting (heating).
                    # new_set_point = max_set_point - (cleared_price_ratio * (
                    #            max_set_point - min_set_point))
                    self.direct_actuate(model.actuation_topic, new_set_point)

    def direct_actuate(self, target_id, new_set_point):
        pass

    def release(self, mkt):
        super(DirectRatioActuationManager, self).release(mkt)
        for model in self.parent.models.values():  # TODO: Generalize this away from models in ModelFrameAsset.
            if model.actuation_topic:
                self.direct_release(model.actuation_topic)

    def direct_release(self, target_id):
        pass
