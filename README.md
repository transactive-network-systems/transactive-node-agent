# TransactiveNodeAgent

A VOLTTRON agent that runs one TENT transactive node. `TransactiveNodeAgent` subclasses both the VOLTTRON `Agent` and
`tent.transactive_node.TransactiveNode`: TENT provides the markets, neighbors, assets, meter points and information
services; the agent provides their connection to the message bus (signals, device data, archive records) and runs the
market state machine in a greenlet.

The package also ships the VOLTTRON-specific components a node configuration can name:

| Class | Module | Role |
|---|---|---|
| `VolttronDataManager` | `transactive_node.data_manager` | Registers archive tables with the TNS historian over RPC and publishes archive records to the historian's topic. |
| `TNSMeterPoint` | `transactive_node.tns_meter_point` | A meter point fed by a `devices/` publication: one named point of the all-points message. |
| `WeatherDotGovTemperatureForecastModel` | `transactive_node.weather_dot_gov_temperature_forecast_model` | Hourly temperature forecast from the VOLTTRON weather.gov agent, by RPC. |
| `TNSDirectRatioActuationManager` | `transactive_node.local_asset.actuation_manager.tns_direct_ratio` | Sets and reverts points through the platform actuator agent. |
| `ILCActuationManager` | `transactive_node.local_asset.actuation_manager.ilc` | Publishes each interval's scheduled power as a demand target for an ILC agent. |

## Installation

TENT (`code/TENT` in this project) is not on PyPI. Install it into the platform's environment first, then the agent:

```shell
pip install -e ../TENT
vctl install . --vip-identity tn.campus --agent-config sample_config/campus.json --start
```

Python 3.11 or newer. Dependencies are declared in `pyproject.toml`.

## Configuration

The agent reads one configuration-store entry, `config`. It is a JSON object with the agent's own keys, TENT's node
keys and the component lists. The files in `sample_config/` are the four node configurations the container deployment
runs; `sample_config/weather_dot_gov_temperature_forecast_model.json` is one `information_service_models` entry.

### Agent keys

| Key | Default | Meaning |
|---|---|---|
| `db_topic` | `TNS` | Root of every topic the agent derives (archive and signal topics below). The deployment uses `tns`. |
| `subscribe_all_platforms` | `false` | Subscribe to the derived and configured topics on every platform of the federation, not only the local one. |
| `topics` | `{}` | Map of subscribe keys to topics. Components name a key (for example a meter point's `subscribe_key`); the agent subscribes to the topic. Entries here also override any derived topic of the same key; the value `IGNORE` disables a publish key. |

### Node keys (TENT)

| Key | Default | Meaning |
|---|---|---|
| `name` | `""` | The node's name. It is part of the derived topics and must match what neighbors call this node. |
| `description` | `""` | Free text, archived. |
| `tz` | platform zone | IANA zone name of the node (`UTC`, `America/Los_Angeles`). Configured naive times are read in this zone. |
| `simulation` | `false` | Run on a simulated clock. |
| `simulation_start_time` | | Simulated time at the anchor. Required when `simulation` is true. |
| `simulation_one_hour_in_seconds` | `3600` | Real seconds per simulated hour. |
| `simulation_anchor_time` | node creation time | Real time at which simulated time equals `simulation_start_time`. Set it to the same value on every node of one simulation so nodes started at different times agree on the simulated time. |
| `loop_sleep_duration` | `0.01` | Seconds the state-machine loop yields after each market's events and after each pass. |
| `actor_id`, `organization_id` | `null` | Identity of the actor this node represents and of its business entity; archived. |

### `data_manager`

The archive. The deployment uses `VolttronDataManager`:

```json
"data_manager": {
  "class_name": "VolttronDataManager",
  "module_name": "transactive_node.data_manager",
  "historian_vip": "tns.historian",
  "rpc_timeout": 30
}
```

| Key | Default | Meaning |
|---|---|---|
| `historian_vip` | required | VIP identity of the transactive-node historian. The node calls its `create_tables` RPC with every archive table definition it needs before publishing. |
| `rpc_timeout` | `30` | Seconds to wait for the historian's reply. |
| `archive_key` | `archive` | Publish key the records go out under (see topics below). |

Records produced before the historian has acknowledged the tables are queued and published as one batch when it has.
A historian error reply is raised, not logged away.

`LocalDataManager` from TENT (`tent.data_manager.local_data_manager`, with `db_params`) writes to a database directly
and is what the city and small-office samples use.

### Component lists

`meter_points`, `information_service_models`, `local_assets`, `neighbors` and `market_series` are lists of component
configurations. Each entry names its class with `class_name` and `module_name` and passes the rest of its keys to the
class's constructor; `market_series` entries also carry `market_class_name` and `market_module_name`. The keys of the
TENT classes are documented in TENT; the agent's own components take:

- `TNSMeterPoint`: `subscribe_key` (a key of `topics`), `point_name` (the point to read out of the device message),
  `external_platform`, plus TENT's `MeterPoint` keys (`measurement_type`, `measurement_unit`, `measurement_interval`,
  `data_period_seconds`, `max_buffer_seconds`, `scale_factor`). A value of `0.0` is a reading; a message that is not a
  mapping or has no such point is logged and ignored.
- `WeatherDotGovTemperatureForecastModel`: `weather_vip` (the weather agent's identity, required), `location` (one
  weather.gov location dictionary, required: `{"lat": .., "long": ..}`, `{"station": "KPSC"}` or
  `{"wfo": .., "x": .., "y": ..}`), `external_platform` (when the weather agent runs on another platform of the
  federation), `rpc_timeout` (seconds, default 15), `temperature_point_name` (default `air_temperature`, the weather
  agent's standard name for NOAA's `temperature`), `max_gap_time` (seconds an interval may borrow a neighbouring
  forecast hour, default four hours) and `tmy_fallback_path` (a CSV with `Timestamp` and `Value` columns used when the
  service fails or does not cover the market). One RPC is made per update; nothing retries. The service forecasts
  real time, so a simulated run uses TENT's `TemperatureForecastModel` with `weather_file_path` instead.
- `TNSDirectRatioActuationManager` (inside a local asset's `actuation_manager`): `actuator_identity`, the agent that
  answers `set_point` and `revert_point`.
- `ILCActuationManager`: `ilc_target_topic`, the topic the ILC agent reads targets from.

### Applying a changed configuration

Storing a new `config` (an UPDATE) rebuilds the node: the state-machine greenlet is stopped, every subscription of the
previous configuration is removed, all components are dropped, and the node is built again from the new contents. The
rebuilt node is a new run with a new archive identity, and without `simulation_anchor_time` its simulated clock is
re-anchored, exactly as a restart would.

## Topics

With `db_topic` D, node name N and a neighbor named B:

| Topic | Direction | Content |
|---|---|---|
| `D/N/archive` | published | Batches of archive records (`{"table_name", "data"}` envelopes) for the historian. |
| `D/N/B` | published | Transactive signals from this node to neighbor B. |
| `D/B/N` | subscribed | Transactive signals from neighbor B to this node. |
| `topics[key]` | subscribed | Device data for the component that names `key`; `devices/` and `analysis/` messages are unwrapped to their first element (the point values). |

Publications carry `Date` and `TimeStamp` headers with the node's current time (simulated when simulating). On
subscriptions the `TimeStamp` header is the time of the reading; a missing header falls back to the node's clock, a
naive timestamp is rejected. A subscription to a topic receives that topic and topics below it only: a subscription
to `.../HP1` does not receive `.../HP10`, although the platform matches both by prefix.

## Tests

The tests run without a platform. `tests/conftest.py` installs fake `volttron` and `gevent` modules when the real ones
are not importable and a fake `Agent` whose `vip` and `core` record what the agent does; the fake pubsub delivers by
the platform's prefix rule. TENT and pytest must be importable.

```shell
python -m pytest
```

The suite covers the data manager, the subscription wrapper, the state-machine loop, configuration NEW and UPDATE, the
meter point, the ILC actuation manager, the weather.gov model and every file in `sample_config/`.
