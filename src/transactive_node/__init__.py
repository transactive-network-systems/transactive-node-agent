"""VOLTTRON agent that runs a TENT transactive node. HOST_KEYS and strip_host_keys (the agent's own configuration keys)
are importable without the platform; everything else needs volttron."""
from transactive_node.host_keys import HOST_KEYS, strip_host_keys

__all__ = ['HOST_KEYS', 'strip_host_keys']
