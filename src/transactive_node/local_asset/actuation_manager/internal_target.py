import gevent
import logging

from tent.utils.log import setup_logging

from volttron.platform.jsonrpc import RemoteError
from volttron.platform.vip.agent import Agent, errors

setup_logging()
_log = logging.getLogger(__name__)


# TODO: This code should support an Actuation Manager keeping a control loop.
def check_schedule(self, dt):
    if self.actuation_disabled:
        _log.debug("Actuation is disabled!")
        return
    occupied = self.scheduler.check_schedule(dt)

    if occupied and not self.actuation_active:
        self.update_actuation_state(True)
    elif not occupied and self.actuation_active:
        self.update_actuation_state(False)

def update_actuation_state(self, new_state):
    tn = self.tn()
    if self.actuation_active and not bool(new_state):
        # Deactivate actuation: kill periodic and release all values.
        if self.actuation_periodic is not None:
            self.actuation_periodic.kill()
            self.actuation_periodic = None
        for output_info in list(self.outputs.values()):
            self.actuate(output_info["topic"], output_info["release"], output_info["actuator"])
    elif not self.actuation_active and bool(new_state):
        for name, output_info in self.outputs.items():
            # Activate actuation: save prior values, start periodic.
            topic = output_info["topic"]
            if output_info.get("release", None) is not None:
                try:
                    release_value = tn.vip.rpc.call(output_info.get("actuator", "platform.actuator"),
                                                    'get_point',
                                                    topic).get(timeout=10)
                except (RemoteError, gevent.Timeout, errors.VIPError) as ex:
                    _log.warning("Failed to get {} - ex: {}".format(topic, str(ex)))
                    release_value = None
            else:
                release_value = None
            self.outputs[name]["release"] = release_value
        _log.debug("Setup periodic actuation: %s -- %s", self.parent.name, self.control_interval)
        self.actuation_periodic = tn.core.periodic(self.control_interval, self.do_actuation,
                                                   wait=self.control_interval)
    self.actuation_active = new_state

def do_actuation(self, price=None):
    for name, output_info in self.outputs.items():
        if not output_info["condition"]:
            continue
        self.update_outputs(name, price)
        #point = output_info["point"]
        value = output_info.get("value")
        if value is not None and self.scheduler.occupied:
            value = value + output_info["offset"]
            self.actuate(output_info["topic"], value, output_info["actuator"])






# def update_outputs(self, name, price):
#     _log.debug("update_outputs: %s - current_price: %s", self.parent_name, self.current_price)
#     if price is None:
#         if self.current_price is None:
#             return
#         price = self.current_price
#     sets = self.outputs[name]["ct_flex"]
#     if self.actuation_price_range is not None:
#         prices = self.actuation_price_range
#     else:
#         prices = self.determine_prices()
#     if self.demand_limiting:
#         price = max(np.mean(prices), price)
#     _log.debug("Call determine_control: %s", self.parent_name)
#     value = self.determine_control(sets, prices, price)
#     self.outputs[name]["value"] = value
#     point = self.outputs.get("point", name)
#     topic_suffix = "Actuate"
#     message = {point: value, "Price": price}
#     self.publish_record(topic_suffix, message)

# def determine_control(self, sets, prices, price):
#     """
#     prices is an list of 11 elements, evenly spaced from the smallest price
#     to the largest price and corresponds to the y-values of a line.  sets
#     is an np.array of 11 elements, evenly spaced from the control value at
#     the lowest price to the control value at the highest price and
#     corresponds to the x-values of a line.  Price is the cleared price.
#     :param sets: np.array;
#     :param prices: list;
#     :param price: float
#     :return:
#     """
#     _log.debug("determine_control - transactive.py: %s", self.parent_name)
#     control = np.interp(price, prices, sets)
#     control = self.clamp(control, min(self.ct_flexibility), max(self.ct_flexibility))
#     return control
#
# def determine_prices(self):
#     """
#     Determine minimum and maximum price from 24-hour look ahead prices.  If the TNS
#     market architecture is not utilized, this function must be overwritten in the child class.
#     :return:
#     """
#     if self.market_prices and not self.static_price_flag:
#         avg_price = np.mean(self.market_prices)
#         std_price = np.std(self.market_prices)
#         price_min = avg_price - self.price_multiplier * std_price
#         price_max = avg_price + self.price_multiplier * std_price
#     else:
#         avg_price = None
#         std_price = None
#         price_min = self.default_min_price
#         price_max = self.default_max_price
#     _log.debug("Prices: {} - avg: {} - std: {}".format(self.market_prices, avg_price, std_price))
#     price_array = np.linspace(price_min, price_max, 11)
#     return price_array