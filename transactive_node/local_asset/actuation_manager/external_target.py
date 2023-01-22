from transactive_node.local_asset.actuation_manager import ActuationManager


class ExternalTargetActuationManager(ActuationManager):
    def __init(self, **kwargs):
        super(ExternalTargetActuationManager, self).__init__(**kwargs)

    def actuate(self, mkt):
        super(ExternalTargetActuationManager, self).actuate(mkt)
        if self.actuation_active:  # TODO: Add ability to determine future active schedule.
            scheduled_powers_for_mkt = [sp for sp in self.parent.scheduledPowers if sp.market is mkt]
            for sp in scheduled_powers_for_mkt:
                start_time = sp.timeInterval.startTime
                end_time = start_time + sp.timeInterval.duration
                self.set_target(target=sp.value, start=start_time, end=end_time)

    def set_target(self, target, start, end):
        pass
