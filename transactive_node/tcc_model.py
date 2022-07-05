import logging

from datetime import timedelta
from typing import Union

from tent.helpers import find_obj_by_ti, format_timestamp, production
from tent.interval_value import IntervalValue
from tent.local_asset_model import LocalAsset
from tent.market_state import MarketState
from tent.measurement_type import MeasurementType
from tent.timer import Timer
from tent.utils.log import setup_logging
from tent.vertex import Vertex

from volttron.platform.vip.agent.utils import build_agent
from volttron.platform.agent.base_market_agent import MarketAgent
from volttron.platform.agent.base_market_agent.poly_line import PolyLine
from volttron.platform.agent.base_market_agent.point import Point
from volttron.platform.agent.base_market_agent.buy_sell import BUYER
from volttron.platform.agent.base_market_agent.buy_sell import SELLER
from volttron.platform.messaging import headers as headers_mod

setup_logging()
_log = logging.getLogger(__name__)


class TCCModel(LocalAsset):
    # TCCModel - A LocalAssetModel specialization that interfaces integrates
    # the PNNL ILC and/or TCC building systems with the transactive network.
    # TCC - Transactive Control & Coordination: Manages HVAC system load using
    # auctions at the various levels of the HVAC system (e.g., VAV boxes,
    # chillers, etc.)
    # ILC - Integrated Load Control: Originally designed to limit total
    # building load below a prescribed threshold. Has been recently modified
    # to make the threshold price-responsive.
    # This class necessarily redefines two methods: schedule_power() and
    # update_vertices(). The methods call a "single market function" that was
    # developed by Sen Huang at PNNL. This function simulates (using Energy
    # Plus) building performance over a 24-hour period and replies with a
    # series of records for each time interval. The records represent the
    # minimum and maximum inflection points and are therefore very like Vertex
    # objects.

    def __init__(self,
                 base_tcc_market_name: str = 'electric',
                 building_market_default_price: float = 0.5,
                 max_deliver_capacity: float = 0.0,
                 mix_market_duration: Union[float, int, timedelta] = timedelta(minutes=20),
                 real_time_market_name: str = 'refinement_electric',
                 tcc_interval_count: int = 24,
                 *args, **kwargs):
        super(TCCModel, self).__init__(*args, **kwargs)

        self.base_tcc_market_name = str(base_tcc_market_name)  # Need to agree on this with other market agents
        self.building_market_default_price = float(building_market_default_price)
        self.max_deliver_capacity = float(max_deliver_capacity)
        self.mix_market_duration = mix_market_duration if isinstance(mix_market_duration, timedelta)\
            else timedelta(seconds=mix_market_duration)
        self.real_time_market_name = str(real_time_market_name)
        self.tcc_interval_count = int(tcc_interval_count)

        # These properties and lists are to be dynamically assigned. An implementer would usually not manually assign
        # these properties.
        self.building_demand_curves = [None]*self.tcc_interval_count
        self._building_market_prices = [self.building_market_default_price]*self.tcc_interval_count
        _log.info("Initial price: {}".format(self._building_market_prices))
        self.current_day_ahead_market_name = None
        self.day_ahead_mixmarket_running = False
        self.day_ahead_clear_price_sent = {}
        self.mix_market_running = False
        self.prices = [None]*self.tcc_interval_count
        self.quantities = [None]*self.tcc_interval_count
        self.real_time_clear_price_sent = {}
        self.real_time_mix_market_running = False
        self.real_time_price = [None]*2
        self.real_time_quantity = [None]*2
        self.real_time_building_demand_curve = [None]*2
        self.tcc_curves = None
        self.tcc_market_names = ['_'.join([self.base_tcc_market_name, str(i)]) for i in range(self.tcc_interval_count)]
        self.tnt_real_time_market = None

        # TODO: Does the self.name in the next line need to be tn.name?
        self.market_balanced_price_topic = "{}/{}/market_balanced_prices".format(self.tn().db_topic, self.tn().name)
        self.cleared_price_topic = 'tnc/cleared_prices/{}'.format(self.name)

        # Create TCC Market Agent
        self.tcc_agent: MarketAgent = build_agent(agent_class=MarketAgent)

        self.tn().vip.pubsub.subscribe(peer='pubsub',
                                       prefix=self.market_balanced_price_topic,
                                       callback=self.send_cleared_price)

        # Join electric mix-markets
        for market in self.tcc_market_names:
            self.tcc_agent.join_market(market, SELLER, self.reservation_callback, self.electric_offer_callback,
                                       self.aggregate_callback, self.price_callback, self.error_callback)

        # Join real time market
        self.tcc_agent.join_market(self.real_time_market_name, SELLER, self.real_time_reservation_callback,
                                   self.real_time_offer_callback, self.real_time_aggregate_callback,
                                   self.real_time_price_callback, self.error_callback)

    def start_real_time_mixmarket(self, resend_balanced_prices=False, mkt=None):
        tn = self.tn()
        self.real_time_price = [None]
        self.real_time_quantity = [None]

        # TODO: Did this work here? Moved from state_machine_loop.
        # Clear old self.real_time_clear_price_sent of expired markets.
        # for market_name in self.real_time_clear_price_sent:
        #     if not tn.get_market_by_name(market_name):
        #         del self.real_time_clear_price_sent[market_name]
        self.real_time_clear_price_sent = {k: v for k, v in self.real_time_clear_price_sent.items() if
                                           k in [m.name for m in tn.markets]}

        _log.debug("Building start_realtime_mixmarket: market name: {}".format(mkt.name))
        if mkt is None:
            # Balance market with previous known demands
            self.tnt_real_time_market = tn.markets[0]  # If mkt is None, Assume only 1 TNS market per node
        else:
            self.tnt_real_time_market = mkt

        # Check if now is near the end of the hour, applicable only if not in simulation mode
        now = Timer.get_cur_time()
        near_end_of_hour = False
        if not tn.simulation:
            near_end_of_hour = self.near_end_of_hour(now)

        self.tnt_real_time_market.check_marginal_prices(self)

        _log.debug("Building start_realtime_mixmarket for name: {}: converged: {}, resend_balanced_prices: {},"
                   " near_end_of_hour: {}".format(
                                                  self.tnt_real_time_market.name,
                                                  self.tnt_real_time_market.converged,
                                                  resend_balanced_prices,
                                                  near_end_of_hour
                                                  ))
        _log.debug("Building start_realtime_mixmarket for name: {},"
                   " marginal prices: {}".format(self.tnt_real_time_market.name,
                                                 self.tnt_real_time_market.marginalPrices))
        if not self.tnt_real_time_market.converged or resend_balanced_prices:
            self.real_time_clear_price_sent[self.tnt_real_time_market.name] = False
            _log.info("Building start_realtime_mixmarket:"
                      " here1: {}".format(self.real_time_clear_price_sent[self.tnt_real_time_market.name]))
            # Get price of the hour
            price = self.tnt_real_time_market.marginalPrices
            _log.debug("Building start_realtime_mixmarket: price: {}".format(price[0].value))
            initial_price = self.tnt_real_time_market.marginalPrices
            _log.debug("Building start_realtime_mixmarket:"
                       " initial startTime : {}".format(initial_price[0].timeInterval.startTime))
            avg_price, std_dev = self.tnt_real_time_market.model_prices(initial_price[0].timeInterval.startTime)
            prices_tuple = [(avg_price, std_dev)]

            self.real_time_price = [price[0].value]
            market_start_hour = price[0].timeInterval.startTime.hour
            time_intervals = [price[0].timeInterval.startTime.strftime('%Y%m%dT%H%M%S')]
            # Signal to start mix market only if the previous market is done
            if not self.real_time_mix_market_running and not near_end_of_hour:
                _log.debug("Building start_realtime_mixmarket: here2")
                self.real_time_mix_market_running = True
                # Update weather information
                weather_service = None
                if len(tn.informationServiceModels) > 0:
                    weather_service = tn.informationServiceModels[0]
                    weather_service.update_information(self.tnt_real_time_market)
                _log.info("Market name: {} start_realtime_mixmarket Building At market START Market"
                          " marginal prices are: {}".format(mkt.name, self.real_time_price))
                if weather_service is None:
                    # TODO: Should this use tcc_agent?
                    tn.vip.pubsub.publish(peer='pubsub',
                                          topic='mixmarket/start_new_cycle',
                                          message={"prices": self.real_time_price,
                                                   "price_info": prices_tuple,
                                                   "market_intervals": time_intervals,
                                                   "Date": format_timestamp(now),
                                                   "correction_market": True})
                else:
                    temps = [x.value for x in weather_service.predictedValues
                             if x.timeInterval.startTime.hour == market_start_hour]
                    # temps = temps[-24:]
                    _log.debug("temps are {}".format(temps))
                    # TODO: Should this use tcc_agent?
                    tn.vip.pubsub.publish(peer='pubsub',
                                      topic='mixmarket/start_new_cycle',
                                      message={"prices": self.real_time_price,
                                               "price_info": prices_tuple,
                                               "market_intervals": time_intervals,
                                               "temp": temps,
                                               "Date": format_timestamp(now),
                                               "correction_market": True})

                # TODO: Remove this! Testing only!
                self.real_time_reservation_callback(timestamp="2021-07-02 00:00:00",
                                          market_name="electric_0",
                                          buyer_seller="seller")
                self.real_time_offer_callback(timestamp="2021-07-02 00:00:00",
                                          market_name="electric_0",
                                          buyer_seller="seller")
                supply_curve = PolyLine()
                supply_curve.add(Point(quantity=19.467139937430645, price=9.875727030740128))
                supply_curve.add(Point(quantity=19.467139937430645, price=7.999302698874049))
                self.real_time_aggregate_callback(timestamp=Timer.get_cur_time(),
                                                  market_name=self.real_time_market_name,
                                                  buyer_seller='buyer',
                                                  aggregate_demand=supply_curve)
                self.real_time_price_callback(timestamp=Timer.get_cur_time(),
                                              market_name=self.real_time_market_name,
                                              buyer_seller='buyer',
                                              price=9.079124841003399,
                                              quantity=19.46335661743057)

    # 191219DJH: Consider the interactions of mixed market with the market state machine, please.
    #            I'm finding it very hard to determine which functions address the mixed market, and which address the
    #            network market(s). This is confusing. All building functions should have been addressed by a building
    #            asset model.
    #            IMPORTANT: This must be rethought still again when there are multiple and correction markets. THERE IS
    #                       NOT SIMPLY ONE OBJECT "MARKET".
    def start_mixmarket(self, resend_balanced_prices=False, mkt=None):
        tn = self.tn()
        # Reset price array
        self.prices = [None]*self.tcc_interval_count

        # Save the 1st quantity as prior 2nd quantity
        # cur_quantity = self.quantities[1]
        # cur_curve = self.building_demand_curves[1]

        # Reset quantities and curves
        self.quantities = [None]*self.tcc_interval_count
        self.building_demand_curves = [None]*self.tcc_interval_count

        # TODO: Did this work here? Moved from state_machine_loop.
        # Clear old self.day_ahead_clear_price_sent of expired markets.
        # for market_name in self.day_ahead_clear_price_sent:
        #     if not tn.get_market_by_name(market_name):
        #         del self.day_ahead_clear_price_sent[market_name]
        self.day_ahead_clear_price_sent = {k: v for k, v in self.day_ahead_clear_price_sent.items() if
                                           k in [m.name for m in tn.markets]}

        # If new cycle, set the 1st quantity to the corresponding quantity of previous hour
        # if start_of_cycle:
        #     self.quantities[0] = cur_quantity
        #     self.building_demand_curves[0] = cur_curve

        if mkt is None:
            # Balance market with previous known demands
            market = tn.markets[0]  # If mkt is None, Assume only 1 TNS market per node
        else:
            market = mkt
            self.current_day_ahead_market_name = mkt.name
        market.signal_new_data = True

        # Check if now is near the end of the hour, applicable only if not in simulation mode
        now = Timer.get_cur_time()
        near_end_of_hour = False
        if not tn.simulation:
            near_end_of_hour = self.near_end_of_hour(now)

        market.check_marginal_prices(self)

        _log.debug("Building start_mixMarket for name: {}: converged: {}, resend_balanced_prices: {},"
                   " near_end_of_hour: {}".format(
                                                  market.name,
                                                  market.converged,
                                                  resend_balanced_prices,
                                                  near_end_of_hour
                                                  ))
        _log.debug("Building start_mixMarket for name: {}, marginal prices: {}".format(market.name,
                                                                                       market.marginalPrices))
        if not market.converged or resend_balanced_prices:
            self.day_ahead_mixmarket_running = True
            self.day_ahead_clear_price_sent[market.name] = False
            _log.info("Building start_mixMarket: here1, flag: {}".format(self.day_ahead_clear_price_sent[market.name]))
            prices = market.marginalPrices
            initial_prices = market.marginalPrices
            prices_tuple = list()
            time_intervals = list()
            for x in range(len(initial_prices)):
                avg_price, std_dev = market.model_prices(initial_prices[x].timeInterval.startTime)
                prices_tuple.append((avg_price, std_dev))
                time_intervals.append(initial_prices[x].timeInterval.startTime.strftime('%Y%m%dT%H%M%S'))

            for idx, p in enumerate(prices):
                self._building_market_prices[idx] = p.value
            self.prices = self._building_market_prices  # [p.value for p in prices]

            # Signal to start mix market only if the previous market is done
            if not self.mix_market_running and not near_end_of_hour:
                _log.debug("Building start_mixMarket: here2")
                self.mix_market_running = True
                # Update weather information
                weather_service = None
                if len(tn.informationServiceModels) > 0:
                    weather_service = tn.informationServiceModels[0]
                    weather_service.update_information(market)
                _log.info("Market name: {} Building At market START Market marginal prices are: {}".format(mkt.name,
                                                                                                           self.prices))
                if weather_service is None:
                    # TODO: Should this use tcc_agent?
                    tn.vip.pubsub.publish(peer='pubsub',
                                          topic='mixmarket/start_new_cycle',
                                          message={"prices": self.prices,
                                                   "price_info": prices_tuple,
                                                   "market_intervals": time_intervals,
                                                   "Date": format_timestamp(now),
                                                   "correction_market": False})
                else:
                    temps = [x.value for x in weather_service.predictedValues]
                    temps = temps[-self.tcc_interval_count:]
                    _log.debug("temps are {}".format(temps))
                    # TODO: Should this use tcc_agent?
                    tn.vip.pubsub.publish(peer='pubsub',
                                          topic='mixmarket/start_new_cycle',
                                          message={"prices": self.prices,
                                                   "price_info": prices_tuple,
                                                   "market_intervals": time_intervals,
                                                   "temp": temps,
                                                   "Date": format_timestamp(now),
                                                   "correction_market": False})
                # TODO: Remove this! Testing only!
                self.reservation_callback(timestamp="2021-07-02 00:00:00",
                                          market_name="electric_0",
                                          buyer_seller="seller")
                self.electric_offer_callback(timestamp="2021-07-02 00:00:00",
                                          market_name="electric_0",
                                          buyer_seller="seller")
                for i in range(self.tcc_interval_count):
                    supply_curve = PolyLine()
                    supply_curve.add(Point(quantity=19.467139937430645, price=9.875727030740128))
                    supply_curve.add(Point(quantity=19.467139937430645, price=7.999302698874049))
                    self.aggregate_callback(timestamp=Timer.get_cur_time(),
                                            market_name=f'electric_{i}',
                                            buyer_seller='buyer',
                                            aggregate_demand=supply_curve)
                    self.price_callback(timestamp=Timer.get_cur_time(),
                                        market_name=f'electric_{i}',
                                        buyer_seller='buyer',
                                        price=9.079124841003399,
                                        quantity=19.46335661743057)

    def near_end_of_hour(self, now):
        near_end_of_hour = False
        if (now + self.mix_market_duration).hour != now.hour:
            near_end_of_hour = True
            _log.debug("{} did not start mixmarket because it's too late.".format(self.name))

        return near_end_of_hour

    def send_cleared_price(self, peer, sender, bus, topic, headers, message):
        _log.info("At {}, {} receives new cleared prices: {}".format(Timer.get_cur_time(),
                                                                     self.name, message))
        tn = self.tn()
        tnt_market_name = message['tnt_market_name']
        market = tn.get_market_by_name(tnt_market_name)
        tmp_price = []
        time_intervals = list()

        for p in market.marginalPrices:
            tmp_price.append(p.value)
            time_intervals.append(p.timeInterval.startTime.strftime('%Y%m%dT%H%M%S'))

        now = Timer.get_cur_time()
        prices_tuple = list()
        if market.name.startswith('Day-Ahead'):
            for idx, p in enumerate(market.marginalPrices):
                self._building_market_prices[idx] = p.value
                avg_price, std_dev = market.model_prices(p.timeInterval.startTime)
                prices_tuple.append((avg_price, std_dev))
            self.prices = self._building_market_prices  # [p.value for p in prices]
            _log.info(f"Market for name: {market.name} CLEARED marginal prices are: {self.prices},"
                      f" flag: {self.day_ahead_clear_price_sent[market.name]}")

            if not self.day_ahead_clear_price_sent[market.name]:
                # TODO: Should this use tcc_agent?
                tn.vip.pubsub.publish(peer='pubsub',
                                  topic=self.cleared_price_topic,
                                  message={"prices": self.prices,
                                           "price_info": prices_tuple,
                                           "market_intervals": time_intervals,
                                           "Date": format_timestamp(now),
                                           "correction_market": False})
                self.day_ahead_clear_price_sent[market.name] = True
                _log.info(f"Market for name: {market.name},"
                          f" published cleared price {self.day_ahead_clear_price_sent[market.name]}")
        elif market.name.startswith('Real-Time'):
            price = market.marginalPrices
            # Get real time price from Real time market
            self.real_time_price = [price[0].value]
            avg_price, std_dev = market.model_prices(price[0].timeInterval.startTime)
            price_tuple = [(avg_price, std_dev)]
            _log.info(f"Market for name: {market.name} CLEARED marginal price are: {self.real_time_price},"
                      f" flag: {self.real_time_clear_price_sent[market.name]}")
            if not self.real_time_clear_price_sent.get(market.name, False):
                # TODO: Should this use tcc_agent?
                tn.vip.pubsub.publish(peer='pubsub',
                                  topic=self.cleared_price_topic,
                                  message={"prices": self.real_time_price,
                                           "price_info": price_tuple,
                                           "market_intervals": time_intervals,
                                           "Date": format_timestamp(now),
                                           "correction_market": True})

                self.real_time_clear_price_sent[market.name] = True
                _log.info(f"Market for name: {market.name},"
                          f" published cleared price: {self.real_time_clear_price_sent[market.name]}")

    #########################################################################
    # Electric TCC MixMarket methods
    #########################################################################
    def electric_offer_callback(self, timestamp, market_name, buyer_seller):
        if market_name in self.tcc_market_names:
            _log.debug("Building offer_callback: market_name: {}".format(market_name))
            # Get the price for the corresponding market
            idx = int(market_name.split('_')[-1])
            _log.debug("Building offer_callback: prices: {}".format(self.prices))
            price = self.prices[idx]
            # price *= 1000.  # Convert to mWh to be compatible with the mixmarket
            _log.debug("Building offer_callback: market index: {}, price at index: {}".format(idx, price))
            # Quantity
            min_quantity = 0
            max_quantity = 10000  # float("inf")

            # Create supply curve
            supply_curve = PolyLine()
            supply_curve.add(Point(quantity=min_quantity, price=price))
            supply_curve.add(Point(quantity=max_quantity, price=price))

            # Make offer
            _log.debug("{}: offer for {} as {} at {} - Curve: {} {}".format(self.name, market_name, SELLER,
                                                                            timestamp, supply_curve.points[0],
                                                                            supply_curve.points[1]))
            success, message = self.tcc_agent.make_offer(market_name, SELLER, supply_curve)
            _log.debug("{}: offer has {} - Message: {}".format(self.name, success, message))

    def reservation_callback(self, timestamp, market_name, buyer_seller):
        _log.debug("{}: wants reservation for {} as {} at {}".format(self.name,
                                                                     market_name,
                                                                     buyer_seller,
                                                                     timestamp))
        if not self.day_ahead_mixmarket_running:
            _log.debug(f"{self.name}: No reservation for {market_name} as day_ahead_mixmarket_running:"
                       f" {self.day_ahead_mixmarket_running}")
            return False
        else:
            _log.debug(f"{self.name}: reservation made for {market_name} as day_ahead_mixmarket_running:"
                       f" {self.day_ahead_mixmarket_running}")
            return True

    def aggregate_callback(self, timestamp, market_name, buyer_seller, aggregate_demand):
        tn = self.tn()
        if buyer_seller == BUYER and market_name in self.tcc_market_names:  # self.base_tcc_market_name in market_name:
            _log.debug("{}: at ts {} min of aggregate curve : {}".format(self.name,
                                                                         timestamp,
                                                                         aggregate_demand.points[0]))
            _log.debug("{}: at ts {} max of aggregate curve : {}".format(self.name,
                                                                         timestamp,
                                                                         aggregate_demand.points[- 1]))
            _log.debug("At {}: Report aggregate Market: {} buyer Curve: {}".format(Timer.get_cur_time(),
                                                                                   market_name,
                                                                                   aggregate_demand))
            idx = int(market_name.split('_')[-1])
            self.building_demand_curves[idx] = (aggregate_demand.points[0], aggregate_demand.points[-1])
            db_topic = "/".join([tn.db_topic, self.name, "AggregateDemand"])
            message = {
                "Timestamp": format_timestamp(timestamp),
                "MarketName": market_name,
                "Curve": aggregate_demand.points
            }
            headers = {headers_mod.DATE: format_timestamp(Timer.get_cur_time())}
            # TODO: Should this use tcc_agent?
            tn.vip.pubsub.publish("pubsub", db_topic, headers, message).get()

    def price_callback(self, timestamp, market_name, buyer_seller, price, quantity):
        _log.debug("{}: cleared price ({}, {}) for {} as {} at {}".format(Timer.get_cur_time(),
                                                                          price,
                                                                          quantity,
                                                                          market_name,
                                                                          buyer_seller,
                                                                          timestamp))
        tn = self.tn()
        idx = int(market_name.split('_')[-1])
        _log.debug("Market index: {}".format(idx))

        # self.prices[idx+1] = price  # price has 24 values, current hour price is excluded
        self.prices[idx] = price  # price has 24 values, current hour price is excluded
        if price is None:
            raise "Market {} did not clear. Price is none.".format(market_name)
        # idx += 1  # quantity has 25 values while there are 24 future markets
        if self.quantities[idx] is None:
            self.quantities[idx] = 0.
        if quantity is None:
            _log.error("Quantity is None. Set it to 0. Details below.")
            _log.debug("{}: ({}, {}) for {} as {} at {}".format(self.name,
                                                                price,
                                                                quantity,
                                                                market_name,
                                                                buyer_seller,
                                                                timestamp))
            quantity = 0
        self.quantities[idx] += quantity

        _log.debug("At {}, Quantity is {} and quantities are: {}".format(Timer.get_cur_time(),
                                                                         quantity,
                                                                         self.quantities))
        if quantity is not None and quantity < 0:
            _log.error("Quantity received from mixmarket is negative!!! {}".format(quantity))

        # If all markets (ie. exclude 1st value) are done then update demands, otherwise do nothing
        mix_market_done = all([False if q is None else True for q in self.quantities])

        idx = int(market_name.split('_')[-1])
        _log.debug("Mix market done: {}, market idx: {}".format(mix_market_done, idx))

        if mix_market_done:
            self.day_ahead_mixmarket_running = False
            # Check if any quantity is greater than physical limit of the supply wire
            _log.debug("Quantity: {}".format(self.quantities))
            if not all([False if q > self.max_deliver_capacity else True for q in self.quantities]):
                _log.error("One of quantity is greater than "
                           "physical limit {}".format(self.max_deliver_capacity))

            # Check demand curves exist
            all_curves_exist = all([False if q is None else True for q in self.building_demand_curves])
            if not all_curves_exist:
                _log.error("Demand curves: {}".format(self.building_demand_curves))
                raise Exception("Mix market has all quantities but not all demand curves")

            # Update demand and balance market
            self.mix_market_running = False

            curves_arr = [(c[0].tuppleize(), c[1].tuppleize()) if c is not None else None
                          for c in self.building_demand_curves]
            _log.debug("Data at time {}:".format(Timer.get_cur_time()))
            _log.debug("Market intervals: {}".format([x.name for x in tn.markets[0].timeIntervals]))
            _log.debug("Quantities: {}".format(self.quantities))
            _log.debug("Prices: {}".format(self.prices))
            _log.debug("Curves: {}".format(curves_arr))

            db_topic = "/".join([tn.db_topic, self.name, "AggregateDemand"])
            message = {"Timestamp": format_timestamp(timestamp), "Curves": self.building_demand_curves}
            headers = {headers_mod.DATE: format_timestamp(Timer.get_cur_time())}
            # TODO: Should this use tcc_agent?
            tn.vip.pubsub.publish("pubsub", db_topic, headers, message).get()

            # Get TNT market index from current day ahead market name
            tnt_mkt = tn.get_market_by_name(self.current_day_ahead_market_name)

            db_topic = "/".join([tn.db_topic, self.name, "Price"])
            price_message = []
            for i in range(len(tnt_mkt.timeIntervals)):
                ts = tnt_mkt.timeIntervals[i].name
                price = self.prices[i]
                quantity = self.quantities[i]
                price_message.append({'timeInterval': ts, 'price': price, 'quantity': quantity})
            message = {"Timestamp": format_timestamp(timestamp), "Price": price_message}
            headers = {headers_mod.DATE: format_timestamp(Timer.get_cur_time())}
            # TODO: Should this use tcc_agent?
            tn.vip.pubsub.publish("pubsub", db_topic, headers, message).get()

            # SN: Setting TCC curves in local asset model (TccModel)
            self.set_scheduled_power(self.quantities,
                                     self.prices,
                                     self.building_demand_curves,
                                     tnt_mkt)
            for curve in self.building_demand_curves:
                _log.debug("XXX Building curves: {}".format(curve))

    #########################################################################
    # Real Time TCC MixMarket methods
    #########################################################################
    def real_time_reservation_callback(self, timestamp, market_name, buyer_seller):
        _log.debug("{}: wants reservation for {} as {} at {}".format(self.name,
                                                                     market_name,
                                                                     buyer_seller,
                                                                     timestamp))
        if self.day_ahead_mixmarket_running:
            _log.debug(f"{self.name}: No reservation for {market_name} as day_ahead_mixmarket_running:"
                       f" {self.day_ahead_mixmarket_running}")
            return False
        else:
            _log.debug(f"{self.name}: reservation made for {market_name} as day_ahead_mixmarket_running:"
                       f" {self.day_ahead_mixmarket_running}")
            return True

    def real_time_offer_callback(self, timestamp, market_name, buyer_seller):
        # Get price from marginal price of TNT market
        price = self.tnt_real_time_market.marginalPrices
        # Quantity
        min_quantity = 0
        max_quantity = 10000  # float("inf")
        # Create supply curve
        supply_curve = PolyLine()
        supply_curve.add(Point(quantity=min_quantity, price=price[0].value))
        supply_curve.add(Point(quantity=max_quantity, price=price[0].value))

        # Make offer
        _log.debug("{}: offer for {} as {} at {} - Curve: {} {}".format(self.name,
                                                                        market_name,
                                                                        SELLER,
                                                                        timestamp,
                                                                        supply_curve.points[0],
                                                                        supply_curve.points[1]))
        success, message = self.tcc_agent.make_offer(market_name, SELLER, supply_curve)
        _log.debug("{}: offer has {} - Message: {}".format(self.name, success, message))

    def real_time_aggregate_callback(self, timestamp, market_name, buyer_seller, aggregate_demand):
        tn = self.tn()
        if buyer_seller == BUYER and market_name == self.real_time_market_name:
            _log.debug("{}: at ts {} min of aggregate curve : {}".format(self.name,
                                                                         timestamp,
                                                                         aggregate_demand.points[0]))
            _log.debug("{}: at ts {} max of aggregate curve : {}".format(self.name,
                                                                         timestamp,
                                                                         aggregate_demand.points[- 1]))
            _log.debug("At {}: Report aggregate Market: {} buyer Curve: {}".format(Timer.get_cur_time(),
                                                                                   market_name,
                                                                                   aggregate_demand))
            self.real_time_building_demand_curve = [(aggregate_demand.points[0], aggregate_demand.points[-1])]
            db_topic = "/".join([tn.db_topic, self.name, "AggregateDemand"])
            message = {
                "Timestamp": format_timestamp(timestamp),
                "MarketName": market_name,
                "Curve": aggregate_demand.points
            }
            headers = {headers_mod.DATE: format_timestamp(Timer.get_cur_time())}
            # TODO: Should this use tcc_agent?
            tn.vip.pubsub.publish("pubsub", db_topic, headers, message).get()

    def real_time_price_callback(self, timestamp, market_name, buyer_seller, price, quantity):
        _log.debug("{}: cleared price ({}, {}) for {} as {} at {}".format(Timer.get_cur_time(),
                                                                          price,
                                                                          quantity,
                                                                          market_name,
                                                                          buyer_seller,
                                                                          timestamp))
        tn = self.tn()
        if price is not None:
            self.real_time_price = [price]
        if quantity is not None:
            self.real_time_quantity = [quantity]

        market = tn.get_market_by_name(self.tnt_real_time_market.name)

        self.real_time_mix_market_running = False

        curves_arr = [(c[0].tuppleize(), c[1].tuppleize()) if c is not None else None
                      for c in self.real_time_building_demand_curve]
        _log.debug("Real Time market Data at time {}:".format(Timer.get_cur_time()))
        _log.debug("Real Time market Market intervals: {}".format(market.name))
        _log.debug("Real Time market Quantities: {}".format(self.real_time_quantity))
        _log.debug("Real Time market Prices: {}".format(self.real_time_price))
        _log.debug("Real Time market Curves: {}".format(curves_arr))

        db_topic = "/".join([tn.db_topic, self.name, "RealTimeDemand"])
        message = {"Timestamp": format_timestamp(timestamp), "Curves": self.real_time_building_demand_curve}
        headers = {headers_mod.DATE: format_timestamp(Timer.get_cur_time())}
        # TODO: Should this use tcc_agent?
        tn.vip.pubsub.publish("pubsub", db_topic, headers, message).get()

        # SN: Setting TCC curves in local asset model (TccModel)
        self.set_scheduled_power(self.real_time_quantity,
                                 self.real_time_price,
                                 self.real_time_building_demand_curve,
                                 market)

    #########################################################################
    # Shared TCC MixMarket methods
    #########################################################################

    def error_callback(self, timestamp, market_name, buyer_seller, error_code, error_message, aux):
        _log.debug("{}: error for {} as {} at {} - Message: {}".format(self.name,
                                                                       market_name,
                                                                       buyer_seller,
                                                                       timestamp,
                                                                       error_message))

    def set_tcc_curves(self, quantities, prices, curves):
        self.quantities = quantities
        # Ignoring first element since state machine based market does not the correction
        self.tcc_curves = curves
        self.prices = prices
        _log.info("TCC set_tcc_curves are: q: {}, p: {}, c: {}".format(len(self.quantities),
                                                                       len(self.tcc_curves),
                                                                       len(self.prices)))
        _log.info("TCC set_tcc_curves actual demand curves: c: {}".format(self.tcc_curves))

    # SN: Schedule Power by starting Mix market
    def schedule_power(self, mkt):
        _log.info("Market TCC tcc_model schedule_power()")
        if self.tcc_agent is not None and not self.mix_market_running:
            _log.info("Market in {} state..Starting building level mix market ".format(mkt.marketState))
            resend_balanced_prices = False
            # if mkt.marketState == MarketState.DeliveryLead:
            #     resend_balanced_prices = True
            if mkt.marketSeriesName == "Day-Ahead_Auction":
                self.start_mixmarket(resend_balanced_prices, mkt)
            else:
                self.start_real_time_mixmarket(resend_balanced_prices, mkt)


    # SN: Set Scheduled Power after mix market is done
    def set_scheduled_power(self, quantities, prices, curves, mkt):
        self.set_tcc_curves(quantities, prices, curves)
        _log.info("Market {} TCC tcc_model set_scheduled_power()".format(self.name))
        # 200929DJH: This was problematic in Version 3 because there exist perfectly valid scheduled powers in other
        #            markets. Instead, let's calculate any new scheduled powers and eliminate only the scheduled powers
        #            that exist in expired markets.
        # self.scheduledPowers = []
        time_intervals = mkt.timeIntervals

        if self.tcc_curves is not None:
            _log.debug("Market {} TCC tcc_curves is not None, calling update_vertices".format(mkt.name))
            # Curves existed, update vertices first
            self.update_vertices(mkt)
            self.scheduleCalculated = True

        for i in range(len(time_intervals)):
            time_interval = time_intervals[i]
            value = self.defaultPower
            if self.tcc_curves is not None:
                # Update power at this marginal_price
                marginal_price = find_obj_by_ti(mkt.marginalPrices, time_interval)
                marginal_price = marginal_price.value
                value = production(self, marginal_price, time_interval)  # [avg. kW]

            iv = IntervalValue(self, time_interval, mkt, MeasurementType.ScheduledPower, value)
            self.scheduledPowers.append(iv)

        # 200929DJH: This following line is needed to make sure the list of scheduled powers does not grow indefinitely.
        #            Scheduled powers are retained only if their markets have not expired.
        self.scheduledPowers = [x for x in self.scheduledPowers if x.market.marketState != MarketState.Expired]

        sp = [(x.timeInterval.name, x.value) for x in self.scheduledPowers]
        _log.debug("Market TCC scheduledPowers are: {}, length: {}".format(sp, len(sp)))

        if self.scheduleCalculated:
            self.calculate_reserve_margin(mkt)

    def update_vertices(self, mkt):
        if self.tcc_curves is None:
            super(TCCModel, self).update_vertices(mkt)
        else:
            time_intervals = mkt.timeIntervals

            # # 191220DJH: The mixed-market timing seems to be pretty hard-coded. This will not work generally for
            # #            multiple network markets having differing interval durations, numbers of intervals, etc.

            for i in range(len(time_intervals)):
                if self.tcc_curves[i] is None:
                    continue
                # 200925DJH: Clean up active vertices that are to be replaced. This should be fine in Version 3 because
                #            time intervals are unique to their markets.
                try:
                    time_interval = time_intervals[i]
                    self.activeVertices = [x for x in self.activeVertices if x.timeInterval != time_interval]
                    point1 = self.tcc_curves[i][0].tuppleize()
                    q1 = -point1[0]
                    p1 = point1[1]
                    point2 = self.tcc_curves[i][1].tuppleize()
                    q2 = -point2[0]
                    p2 = point2[1]

                    if q2 != q1:
                        v2 = Vertex(p2, 0, q2)
                        iv2 = IntervalValue(self, time_intervals[i], mkt, MeasurementType.ActiveVertex, v2)
                        self.activeVertices.append(iv2)
                        v1 = Vertex(p1, 0, q1)
                        iv1 = IntervalValue(self, time_interval, mkt, MeasurementType.ActiveVertex, v1)
                        self.activeVertices.append(iv1)
                    else:
                        v1 = Vertex(float("inf"), 0, q1)
                        iv1 = IntervalValue(self, time_interval, mkt, MeasurementType.ActiveVertex, v1)
                        self.activeVertices.append(iv1)
                except IndexError as e:
                    _log.debug("TCC model e: {}, i: {}".format(e, i))

        # 200929DJH: This is a good place to make sure the list of active vertices is trimmed and does not grow
        #            indefinitely.
        self.activeVertices = [x for x in self.activeVertices if x.market.marketState != MarketState.Expired]

        av = [(x.timeInterval.name, x.value.marginalPrice, x.value.power) for x in self.activeVertices]
        _log.debug("TCC active vertices are: {}".format(av))
