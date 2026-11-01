"""Test doubles for the platform surface the agent code touches.

The tests run without a VOLTTRON platform. Before any agent module is imported, this conftest installs fake
`volttron` and `gevent` modules into sys.modules when the real ones are not importable, and provides a FakeAgent base
whose `vip` and `core` record what the code under test does. The fakes cover both import paths the agent code tries
(the modular `volttron.client`/`volttron.utils` names and the monolithic `volttron.platform` names).
"""
import sys
import types
from datetime import datetime, timezone

import pytest


# ----------------------------------------------------------------------------------------------------------------------
# Fake message-bus surface
# ----------------------------------------------------------------------------------------------------------------------

class FakeResult:
    def __init__(self, value=None, error=None):
        self.value = value
        self.error = error

    def get(self, timeout=None):
        self.timeout = timeout
        if self.error is not None:
            raise self.error
        return self.value


class FakeRpc:
    """Records calls; replies with `reply` (or raises `error`) from each call's .get()."""
    def __init__(self, reply=None, error=None):
        self.reply = reply
        self.error = error
        self.calls = []

    def call(self, peer, method, *args, **kwargs):
        self.calls.append((peer, method, args, kwargs))
        return FakeResult(self.reply, self.error)


class FakePubSub:
    """Keeps subscriptions and delivers by VOLTTRON's prefix rule (plain startswith), so prefix collisions reproduce."""
    def __init__(self):
        self.subscriptions = []  # (prefix, callback, all_platforms)
        self.unsubscribed = []
        self.published = []      # (topic, headers, message)

    def subscribe(self, peer, prefix, callback, all_platforms=False, **kwargs):
        self.subscriptions.append((prefix, callback, all_platforms))

    def unsubscribe(self, peer, prefix, callback, all_platforms=False, **kwargs):
        self.unsubscribed.append((prefix, callback, all_platforms))
        self.subscriptions = [s for s in self.subscriptions if not (s[0] == prefix and s[1] is callback)]
        return True

    def publish(self, peer, topic, headers=None, message=None, **kwargs):
        self.published.append((topic, headers, message))

    def deliver(self, topic, message, headers=None, sender='fake.sender'):
        """Deliver as the platform would: every callback whose prefix is a string prefix of the topic."""
        for prefix, callback, _ in list(self.subscriptions):
            if topic.startswith(prefix):
                callback('pubsub', sender, '', topic, headers, message)


class FakeConfigStore:
    def __init__(self):
        self.defaults = {}
        self.subscriptions = []

    def set_default(self, name, contents):
        self.defaults[name] = contents

    def subscribe(self, callback, actions=None, pattern=None):
        self.subscriptions.append((callback, actions, pattern))


class FakeVip:
    def __init__(self):
        self.pubsub = FakePubSub()
        self.rpc = FakeRpc(reply={'status': 'success', 'registered_tables': []})
        self.config = FakeConfigStore()


class FakeGreenlet:
    """What the agent needs of a gevent Greenlet: dead, kill(). The target is not run; tests call it directly."""
    def __init__(self, func, args, kwargs):
        self.func, self.args, self.kwargs = func, args, kwargs
        self.dead = False
        self.killed = False

    def kill(self, *args, **kwargs):
        self.killed = True
        self.dead = True


class FakeCore:
    def __init__(self):
        self.spawned = []

    def spawn(self, func, *args, **kwargs):
        greenlet = FakeGreenlet(func, args, kwargs)
        self.spawned.append(greenlet)
        return greenlet

    def spawn_later(self, seconds, func, *args, **kwargs):
        greenlet = FakeGreenlet(func, args, kwargs)
        greenlet.delay = seconds
        self.spawned.append(greenlet)
        return greenlet

    @staticmethod
    def receiver(name):
        def decorator(func):
            return func
        return decorator


class FakeAgentBase:
    """Stands in for volttron's Agent: construction only wires up a fake vip and core."""
    def __init__(self, *args, **kwargs):
        self.vip = FakeVip()
        self.core = FakeCore()


# ----------------------------------------------------------------------------------------------------------------------
# Fake modules: gevent only when absent (the real one works in tests), volttron always
# ----------------------------------------------------------------------------------------------------------------------

def _module(name, **attrs):
    module = types.ModuleType(name)
    for key, value in attrs.items():
        setattr(module, key, value)
    sys.modules[name] = module
    return module


def _install_fake_gevent():
    class Timeout(Exception):
        pass

    def sleep(seconds=0):
        pass

    class AsyncResult:
        """set() then get(); get() on an unset result raises Timeout at once (the fake never blocks)."""
        _unset = object()

        def __init__(self):
            self._value = self._unset

        def set(self, value=None):
            self._value = value

        def ready(self):
            return self._value is not self._unset

        def get(self, block=True, timeout=None):
            if self._value is self._unset:
                raise Timeout()
            return self._value

    class Greenlet:
        """Records the target; never runs it (a looping target would spin with the no-op sleep)."""
        def __init__(self, func, args, kwargs):
            self.func, self.args, self.kwargs = func, args, kwargs
            self.dead = False

        def kill(self, *args, **kwargs):
            self.dead = True

    def spawn(func, *args, **kwargs):
        return Greenlet(func, args, kwargs)

    gevent = _module('gevent', Timeout=Timeout, sleep=sleep, spawn=spawn, Greenlet=Greenlet)
    gevent.event = _module('gevent.event', AsyncResult=AsyncResult)


def _install_fake_volttron():
    class RemoteError(Exception):
        pass

    class VIPError(Exception):
        pass

    class Unreachable(VIPError):
        pass

    errors = _module('volttron.client.vip.agent.errors', VIPError=VIPError, Unreachable=Unreachable)
    headers = _module('volttron.client.messaging.headers', DATE='Date', TIMESTAMP='TimeStamp', FROM='From')

    def setup_logging(*args, **kwargs):
        pass

    def vip_main(*args, **kwargs):
        raise RuntimeError('vip_main is not available in tests')

    volttron = _module('volttron')
    client = _module('volttron.client')
    _module('volttron.client.logs', setup_logging=setup_logging)
    _module('volttron.client.messaging', headers=headers)
    vip = _module('volttron.client.vip')
    agent = _module('volttron.client.vip.agent', Agent=FakeAgentBase, Core=FakeCore, errors=errors)
    utils = _module('volttron.utils', vip_main=vip_main)
    _module('volttron.utils.jsonrpc', RemoteError=RemoteError)
    # Monolithic names, for the fallback import branch.
    platform = _module('volttron.platform')
    _module('volttron.platform.jsonrpc', RemoteError=RemoteError)
    _module('volttron.platform.messaging', headers=headers)
    _module('volttron.platform.messaging.headers', DATE='Date', TIMESTAMP='TimeStamp', FROM='From')
    _module('volttron.platform.agent', utils=_module('volttron.platform.agent.utils',
                                                        setup_logging=setup_logging, vip_main=vip_main))
    _module('volttron.platform.vip', agent=agent)
    sys.modules['volttron.platform.vip.agent'] = agent
    volttron.client, volttron.utils, volttron.platform = client, utils, platform
    client.vip, client.messaging, client.logs = vip, sys.modules['volttron.client.messaging'], sys.modules['volttron.client.logs']
    vip.agent = agent
    agent.errors = errors


try:
    import gevent  # noqa: F401
except ImportError:
    _install_fake_gevent()

# The agent tests never want a real platform: the fake Agent is installed whether or not volttron is importable.
_install_fake_volttron()


# ----------------------------------------------------------------------------------------------------------------------
# Shared fixtures
# ----------------------------------------------------------------------------------------------------------------------

@pytest.fixture
def fixed_now(monkeypatch):
    """Pin Timer.now() to one aware instant so time arithmetic in the code under test is deterministic."""
    from tent.utils.timer import Timer
    instant = datetime(2026, 10, 31, 12, 0, tzinfo=timezone.utc)
    monkeypatch.setattr(Timer, 'now', classmethod(lambda cls, tz=None: instant.astimezone(tz) if tz else instant))
    return instant


class FakeDataManager:
    def __init__(self):
        self.records = []

    def archive_data(self, records):
        self.records.extend([records] if isinstance(records, dict) else list(records))


class FakeNode:
    """What a TENT component needs of its transactive node when tested on its own."""
    def __init__(self, tz='UTC', simulation=False):
        from uuid6 import uuid7
        self.id = uuid7()
        self.name = 'TestNode'
        self.tz = tz
        self.simulation = simulation
        self.data_manager = FakeDataManager()
        self.vip = FakeVip()
        self.subscriptions = {}

    def register_subscription(self, subscribe_key, callback):
        self.subscriptions[subscribe_key] = callback


@pytest.fixture
def node():
    return FakeNode()


@pytest.fixture
def simulated_node():
    return FakeNode(simulation=True)


@pytest.fixture
def node_factory():
    return FakeNode
