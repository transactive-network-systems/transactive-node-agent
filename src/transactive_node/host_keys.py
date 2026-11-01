"""
Copyright (c) 2022, Battelle Memorial Institute
All rights reserved.
Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:
1. Redistributions of source code must retain the above copyright notice, this
   list of conditions and the following disclaimer.
2. Redistributions in binary form must reproduce the above copyright notice,
   this list of conditions and the following disclaimer in the documentation
   and/or other materials provided with the distribution.
THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
The views and conclusions contained in the software and documentation are those
of the authors and should not be interpreted as representing official policies,
either expressed or implied, of the FreeBSD Project.
This material was prepared as an account of work sponsored by an agency of the
United States Government. Neither the United States Government nor the United
States Department of Energy, nor Battelle, nor any of their employees, nor any
jurisdiction or organization that has cooperated in th.e development of these
materials, makes any warranty, express or implied, or assumes any legal
liability or responsibility for the accuracy, completeness, or usefulness or
any information, apparatus, product, software, or process disclosed, or
represents that its use would not infringe privately owned rights.
Reference herein to any specific commercial product, process, or service by
trade name, trademark, manufacturer, or otherwise does not necessarily
constitute or imply its endorsement, recommendation, or favoring by the
United States Government or any agency thereof, or Battelle Memorial Institute.
The views and opinions of authors expressed herein do not necessarily state or
reflect those of the United States Government or any agency thereof.

PACIFIC NORTHWEST NATIONAL LABORATORY
operated by BATTELLE for the UNITED STATES DEPARTMENT OF ENERGY
under Contract DE-AC05-76RL01830
"""
"""The configuration keys the TransactiveNodeAgent owns.

A node configuration is one document, but two readers consume it: TENT builds the node from it, and the agent reads the
keys that describe the node's transport on the message bus. TENT's configuration models reject keys they do not know,
so the agent takes its own keys out before TENT sees the document. HOST_KEYS is the one statement of which keys those
are; the agent's own popping goes through strip_host_keys, and tools that validate a node configuration with TENT's
models (tent-analysis) strip the same keys first. This module imports nothing from the platform, so it can be imported
wherever the agent package is installed.
"""
from collections.abc import Mapping
from copy import deepcopy
from types import MappingProxyType

__all__ = ['HOST_KEYS', 'strip_host_keys']

HOST_KEYS: Mapping[str, frozenset[str]] = MappingProxyType({
    # Top-level keys of the node configuration.
    '': frozenset({
        'db_topic',                 # root of the message-bus topics the agent derives
        'subscribe_all_platforms',  # subscribe across the federation's platforms
        'topics',                   # subscribe key -> topic
        'slow_iteration_warning',   # [s] state-machine iteration length that is logged as slow
    }),
    # Keys of the entries of a node list (list name -> keys).
    'neighbors': frozenset({
        'sender',                   # identity the platform must present for this neighbor's signals
    }),
})


def strip_host_keys(configuration: Mapping) -> tuple[dict, dict]:
    """
    Split a node configuration into what TENT reads and what the agent reads.

    Returns ``(tent_configuration, host_values)``: a deep copy of `configuration` without any HOST_KEYS key, and the
    values taken out as ``{'': {key: value, ...}, 'neighbors': {entry name: {key: value, ...}, ...}}`` (only the keys
    that were present). Nothing else is removed or changed, so a key that is neither TENT's nor the agent's still
    reaches TENT and is rejected there.
    """
    if not isinstance(configuration, Mapping):
        raise TypeError(f'A node configuration is a mapping, not {type(configuration).__name__}.')
    remaining = deepcopy(dict(configuration))
    host_values: dict = {'': {}}
    for key in HOST_KEYS['']:
        if key in remaining:
            host_values[''][key] = remaining.pop(key)
    for list_name, keys in HOST_KEYS.items():
        if not list_name:
            continue
        host_values[list_name] = {}
        for entry in remaining.get(list_name) or []:
            if not isinstance(entry, dict):
                continue
            taken = {key: entry.pop(key) for key in keys if key in entry}
            if taken:
                host_values[list_name][entry.get('name')] = taken
    return remaining, host_values
