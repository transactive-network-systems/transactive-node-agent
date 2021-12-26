import logging
import weakref
from typing import Iterable

from volttron.platform.agent import utils

from ...TNT_Version3.PyCode.neighbor_model import Neighbor
from ...TNT_Version3.PyCode.timer import Timer
from ...TNT_Version3.PyCode.vertex import Vertex
from ...TNT_Version3.PyCode.direction import Direction

utils.setup_logging()
_log = logging.getLogger(__name__)


class TNSNeighbor(Neighbor):
    def __init__(self):
        super(TNSNeighbor, self).__init__()
        self.tn = None
        self.subscribeTopic = None

    def configure(self, transactive_node, config):
        self.tn = weakref.proxy(transactive_node)
        self.convergenceThreshold = float(config.get('convergence_threshold', self.convergenceThreshold))
        self.costParameters = [float(cp) for cp in config.get('cost_parameters', self.costParameters)]
        self.demandMonth = int(config.get('demand_month', self.demandMonth))
        self.demandRate = float(config('demand_rate', self.demandRate))
        self.demandThreshold = float(config.get('demand_threshold', self.demandThreshold))
        self.demandThresholdCoef = float(config.get('demand_threshold_coef', self.demandThresholdCoef))
        self.description = str(config.get('description', self.description))
        self.effectiveImpedance = float(config.get('effective_impedance', self.effectiveImpedance))
        friend = config.get('friend', self.friend)
        if not (isinstance(friend, bool) or isinstance(friend, int) or isinstance(friend, float)):
            raise ValueError(f'Configured parameter "friend" must be bool, not {type(self.friend)}')
        self.friend = bool(friend)
        self.location = str(config.get('location', self.location))
        self.lossFactor = float(config.get('loss_factor', self.lossFactor))
        self.maximumPower = float(config.get('maximum_power', self.maximumPower))
        self.mechanism = str(config.get('mechanism', self.mechanism))
        self.minimumPower = float(config.get('minimum_power', self.minimumPower))
        self.name = str(config.get('name', self.name))
        self.subclass = config.get('subclass', self.subclass)
        transactive = config.get('transactive', self.transactive)
        if not (isinstance(transactive, bool) or isinstance(transactive, int) or isinstance(transactive, float)):
            raise ValueError(f'Configured parameter "transactive" must be bool, not {type(self.transactive)}')
        self.transactive = bool(transactive)
        self.upOrDown = Direction[config.get('up_or_down', self.upOrDown)]
`
        # TODO: ABOVE ARE ARGUMENTS TO CONSTRUCTOR OF BASE CLASS, BELOW ARE NOT.
        subscription_topic_postfix = str(config.get('subscription_topic_postfix', ''))
        s_topic = f'{self.tn.db_topic}/{self.name}/{self.tn.name}'
        self.subscribeTopic = s_topic if not subscription_topic_postfix else f'{s_topic}/{subscription_topic_postfix}'
        publication_topic_postfix = str(config.get('publication_topic_postfix', ''))
        p_topic = f'{self.tn.db_topic}/{self.tn.name}/{self.name}'
        self.publishTopic = p_topic if not publication_topic_postfix else f'{p_topic}/{publication_topic_postfix}'

        vertices = config.get('default_vertices', self.defaultVertices)
        if not all([isinstance(vertex, Vertex) for vertex in vertices]):
            if all([isinstance(vertex, list) for vertex in vertices]):
                self.defaultVertices = []
                for vertex in vertices:
                    self.defaultVertices.append(Vertex(*[float(i) for i in vertex]))
            elif all([isinstance(vertex, dict) for vertex in vertices]):
                self.defaultVertices = []
                for vertex in vertices:
                    continuity = vertex.get('continuity', True)
                    if not isinstance(continuity, bool) or isinstance(continuity, int) or isinstance(continuity, float):
                        raise ValueError(f'Default vertex parameter "continuity" must be bool, not {type(continuity)}')
                    v = Vertex(marginal_price=float(vertex['marginal_price']),
                               prod_cost=float(vertex['production_cost']),
                               power=float(vertex['power']),
                               continuity=bool(continuity),
                               power_uncertainty=float(vertex.get('power_uncertainty', 0.0)),
                               record=int(vertex.get('record', 0))
                               )
            else:
                raise ValueError(f'Configured default vertices for {self.name} neighbor must all be lists or dicts.'
                                 f' Received: {vertices}')

        meter_points = config.get('meter_points', self.meterPoints)
        if meter_points:
            available_mps = [mp.name for mp in self.tn.meterPoints]
            for mp in meter_points:
                if mp not in available_mps:
                    raise ValueError(f'Meter point {mp}, required by {self.name} neighbor, is not available.')
                else:
                    self.meterPoints.append(weakref.proxy(self.tn.meterPoints[available_mps.index(mp)]))

        # Subscribe to neighbor publishes
        self.tn.vip.pubsub.subscribe(peer='pubsub',
                                     prefix=self.subscribeTopic,
                                     callback=self.new_transactive_signal,
                                     all_platforms=self.tn.subscribe_all_platforms)
        _log.info(f'{self.tn.name} {self.name} neighbor subscribed to {self.subscribeTopic}')
        _log.debug(f'{self.tn.name} {self.name} neighbor getDict: {self.getDict()}')

    def new_transactive_signal(self, peer, sender, bus, topic, headers, message):
        _log.debug('At {}, {}  receives new transactive signal: {}'.format(Timer.get_cur_time(),
                                                                           self.name, message))
        source = message['source']
        curves = message['curves']
        start_of_cycle = message['start_of_cycle']
        fail_to_converged = message['fail_to_converged']

        self.receivedCurves = curves
        _log.debug(f'{self.tn.name} received new transactive signal from {self.name}:')
        # TODO: Do we need to run a callback on the TN if this is an upstairs neighbor?
