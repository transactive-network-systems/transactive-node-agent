# TransactiveNodeAgent

A VOLTTRON agent that runs one TENT transactive node. `TransactiveNodeAgent` subclasses both the VOLTTRON `Agent` and
`tent.transactive_node.TransactiveNode`: TENT provides the markets, neighbors, assets, meter points and information
services; the agent provides their connection to the message bus (signals, device data, archive records) and runs the
market state machine in a greenlet.

The package also ships the VOLTTRON-specific components a node configuration can name:

| Class | Module | Role |
|---|---|---|
| `VolttronDataManager` | `volttron_archivable.data_manager` (the `volttron-lib-archivable` library, registered by this package's `tent.components` entry points) | Registers archive tables with the Application Historian (RPC or pubsub) and publishes archive records on the node's archive topic. |
| `TNSMeterPoint` | `transactive_node.tns_meter_point` | A meter point fed by a `devices/` publication: one named point of the all-points message. |
| `WeatherDotGovTemperatureForecastModel` | `transactive_node.weather_dot_gov_temperature_forecast_model` | Hourly temperature forecast from the VOLTTRON weather.gov agent, by RPC. |
| `TNSDirectRatioActuationManager` | `transactive_node.local_asset.actuation_manager.tns_direct_ratio` | Sets and reverts points through the platform actuator agent. |
| `ILCActuationManager` | `transactive_node.local_asset.actuation_manager.ilc` | Publishes each interval's scheduled power as a demand target for an ILC agent. |

## Installation

TENT (`code/TENT` in this project) and the archive libraries `lib-archivable` and `volttron-lib-archivable`
(`code/tns-utilities`) are not on PyPI. Install them into the platform's environment first, then the agent:

```shell
pip install -e ../tns-utilities/lib-archivable -e ../tns-utilities/volttron-lib-archivable -e ../TENT
vctl install . --vip-identity tn.campus --agent-config sample_config/campus.json --start
```

Python 3.11 or newer. Dependencies are declared in `pyproject.toml`.

## Configuration

The agent reads one configuration-store entry, `config`. It is a JSON object with the agent's own keys, TENT's node
keys and the component lists. Every block is validated through TENT's configuration models before anything is built
(see TENT's `docs/source/design/configuration_models.rst`): each class below declares a nested `Config` whose fields
are its keys, with types, bounds, defaults and descriptions; an unknown key, a missing required key or a value out of
range is a `ConfigurationError` naming the component and the key, and the running node is left as it was. The
reconfigurable fields (timeouts, the weather and actuator identities, the weather location, the batch settings) can be
changed on a running node; everything else (identities of the historian, the registration path, topics, keys) rebuilds
it. The files in `sample_config/` are the four node configurations the container deployment
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
| `simulation_anchor_time` | `TENT_SIMULATION_ANCHOR_TIME` from the environment, else node creation time | Real time at which simulated time equals `simulation_start_time`. Set it to the same value on every node of one simulation so nodes started at different times agree on the simulated time (see the section on the environment below). |
| `loop_sleep_duration` | `0.01` | Seconds the state-machine loop yields after each market's events and after each pass. |
| `actor_id`, `organization_id` | `null` | Identity of the actor this node represents and of its business entity; archived. |

### `data_manager`

The archive. The deployment uses `VolttronDataManager`:

```json
"data_manager": {
  "class_name": "VolttronDataManager",
  "module_name": "volttron_archivable.data_manager",
  "historian_vip": "tns.historian",
  "external_platform": "central",
  "registration": "pubsub"
}
```

| Key | Default | Meaning |
|---|---|---|
| `historian_vip` | required | VIP identity of the Application Historian (see "Deploying with the Application Historian" below). |
| `registration` | `rpc` | How the node registers its archive table definitions with the historian before publishing: `rpc` or `pubsub` (below). Any other value is an error. |
| `external_platform` | `""` | Platform the historian runs on, for the `rpc` path in a federation. The modular platform has no cross-platform RPC, so a remote historian needs the `pubsub` path; the key then only documents where the historian is. |
| `rpc_timeout` | `30` | Seconds to wait for one answer (an RPC reply or a status message). |
| `rpc_retry_period` | `10` | Seconds between registration attempts while the historian does not answer. |
| `rpc_total_timeout` | `300` | Seconds after which an unanswered registration is given up with an error; nothing is archived in that run. |
| `publish_interval` | `1.0` | Seconds between publications of the queued archive records. |
| `max_batch_records` | `1000` | Largest number of records in one archive message; a flush with more records is split into several messages, in order. |
| `archive_key` | `archive` | Publish key the records go out under (see topics below). |

Registration paths:

- `rpc`: the historian's `create_tables` RPC is called with `{"definitions": [...], "enumerations": [...],
  "schema_version": N}`: the table definitions, the enumerations they name (`{"name", "members"}` each, from
  `tent.enumerations`) and this TENT's `tent.data_manager.ARCHIVE_SCHEMA_VERSION`. It returns the status mapping below. The call is repeated every `rpc_retry_period` seconds until it is answered, for at most `rpc_total_timeout`.
- `pubsub`: the node subscribes to its status topic on all platforms, publishes the same payload with `publisher` (the
  node's name) and a fresh `request_id` on its definitions topic, and waits `rpc_timeout` seconds for the status carrying that id. Without an answer
  it logs an error and publishes again with a new id every `rpc_retry_period` seconds, giving up after
  `rpc_total_timeout`. Status messages for other request ids are logged at debug level and ignored.

The status mapping (the RPC reply and the status message are the same shape):

```json
{
  "request_id": "the request's uuid (null on the RPC path)",
  "publisher": "PNNL_Campus",
  "status": "success | partial | error",
  "registered_tables": ["transactive_node", "market"],
  "rejected_tables": {"meter_point": {"message": "definition differs from the stored one", "differences": "..."}},
  "message": "free text",
  "schema_version": 2
}
```

`schema_version` is the archive schema version the historian's TENT writes; it is logged beside this node's. A
historian whose version differs refuses the registration with `error` and a message naming both versions, so the
node does not start archiving; the two TENT installations must be of the same archive schema version.

Records are not published one at a time. They are queued and published in batches: the backlog once registration
succeeds, then everything queued every `publish_interval` seconds, in messages of at most `max_batch_records` records.
A failed publication is logged and its records are kept for the next batch. Tearing the node down flushes the queue.

`success` turns the archive on and publishes the records queued while waiting. `partial` turns the archive on too,
logs one error per rejected table with its message and differences, and drops every record addressed to a rejected
table, queued or later, with one further error per table name. `error` is raised with the message and the rejected
tables; nothing is archived.

Security note on the `pubsub` path: the platform has no per-publisher capability for pubsub as it has for protected
RPCs, so any agent on any platform of the federation can publish definitions or records under any node's name. The
sender-identity check for signals and archive traffic (audit item A-SEC-01) is deferred by decision of the owner.

`LocalDataManager` from `lib-archivable` (`archivable.local_data_manager`, with `db_params`) writes to a database
directly and is what the city and small-office samples use.

### Component lists

`meter_points`, `information_service_models`, `local_assets`, `neighbors` and `market_series` are lists of component
configurations. Each entry names its class with `class_name` and `module_name` and passes the rest of its keys to the
class's constructor; `market_series` entries also carry `market_class_name` and `market_module_name`. The keys of the
TENT classes are documented in TENT; the agent's own components take:

- `TNSMeterPoint`: `subscribe_key` (a key of `topics`), `point_name` (the point to read out of the device message),
  plus TENT's `MeterPoint` keys (`measurement_type`, `measurement_unit`, `measurement_interval`,
  `data_period_seconds`, `max_buffer_seconds`, `scale_factor`). A value of `0.0` is a reading; a message that is not a
  mapping or has no such point is logged and ignored.
- `WeatherDotGovTemperatureForecastModel`: `weather_vip` (the weather agent's identity, default `platform.weather`), `location` (one
  weather.gov location dictionary, required: `{"lat": .., "long": ..}`, `{"station": "KPSC"}` or
  `{"wfo": .., "x": .., "y": ..}`), `external_platform` (when the weather agent runs on another platform of the
  federation), `rpc_timeout` (seconds, default 15), `temperature_point_name` (default `air_temperature`, the weather
  agent's standard name for NOAA's `temperature`), `max_gap_time` (seconds an interval may borrow a neighbouring
  forecast hour, default four hours) and `tmy_fallback_path` (a CSV with `Timestamp` and `Value` columns used when the
  service fails or does not cover the market). One RPC is made per update; nothing retries. The service forecasts
  real time, so a simulated run uses TENT's `TemperatureForecastModel` with `weather_file_path` instead.
- `TNSDirectRatioActuationManager` (inside a local asset's `actuation_manager`): `actuator_identity`, the agent that
  answers `set_point(path, point_name, value)` and `revert_point(path, point_name)` (the platform actuator, or the
  EnergyPlus agent installed as `platform.actuator` in the containers), and `rpc_timeout` (seconds, default 15). A
  model's `actuation_key` is the point's full topic and is split at its last slash into the device path and the point
  name.
- `ILCActuationManager`: `ilc_target_topic`, the topic the ILC agent reads targets from.

### Applying a changed configuration

Storing a new `config` (an UPDATE) is validated first; a rejected configuration is logged and the running node is not
touched. The running node is then asked to take the new configuration in place (TENT's `reconfigure`): when it differs
from the current one only in reconfigurable fields of existing components, each component applies its changes and
archives a new configuration version, and nothing else happens. Otherwise the node is rebuilt: the state-machine
greenlet is stopped, every subscription of the previous configuration is removed, all components are dropped, and the
node is built again from the new contents. When the simulation settings (`simulation`, `simulation_start_time`, `simulation_one_hour_in_seconds`,
`simulation_anchor_time`, compared as configured) are unchanged, the rebuilt node continues the same run: it keeps its
identity and creation time, its archive row is appended as a new version, and the simulated clock keeps its anchor.
When they changed, the rebuilt node is a new run with a new identity, and without an anchor its simulated clock is
re-anchored. A NEW configuration is always a new run. The log says which happened.

### Shared simulation anchor from the environment

Every node of one simulation should use the same `simulation_anchor_time`. Where the configuration files cannot carry a
per-run value (the container deployment mounts them read-only), set the environment variable
`TENT_SIMULATION_ANCHOR_TIME` for the agent's process to an ISO 8601 time, aware or naive in the node's zone like the
configuration value. It is used when the configuration has no `simulation_anchor_time`; a configured value always wins.
The log records when the anchor came from the environment. An unparsable value fails the configuration.

## Topics

With `db_topic` D, node name N and a neighbor named B:

| Topic | Direction | Content |
|---|---|---|
| `D/archive/N/records` | published | Batches of archive records (`{"table_name", "data"}` envelopes) for the historian. |
| `D/archive/N/definitions` | published | Registration request on the `pubsub` path: `{"publisher", "request_id", "definitions", "schema_version"}`; `publisher` is the node's name, the `<publisher>` segment of the historian's archive topics. |
| `D/archive/N/status` | subscribed, all platforms | The historian's answer to a registration request (status mapping above). |
| `D/N/B` | published | Transactive signals from this node to neighbor B. |
| `D/B/N` | subscribed | Transactive signals from neighbor B to this node. |
| `topics[key]` | subscribed | Device data for the component that names `key`; `devices/` and `analysis/` messages are unwrapped to their first element (the point values). |

Publications carry `Date` and `TimeStamp` headers with the node's current time (simulated when simulating). On
subscriptions the `TimeStamp` header is the time of the reading; a missing header falls back to the node's clock, a
naive timestamp is rejected. A subscription to a topic receives that topic and topics below it only: a subscription
to `.../HP1` does not receive `.../HP10`, although the platform matches both by prefix.

## Deploying with the Application Historian

The Transactive Network System (TNS) archive is the Application Historian (`code/volttron-transactive-node-historian`,
distribution `volttron-application-historian`, class `historian.application.agent.ApplicationHistorian`): a VOLTTRON
historian that stores the nodes' archive records through TENT's `LocalDataManager` and also serves as the platform's
standard historian. Its own README gives the registration contract, the status messages and the configuration keys;
this section is the TNS deployment of it, which the container stack runs on the `central` platform.

Identity. The containers install the historian as `tns.historian` (the agent's default is `platform.historian`), and
every node's `data_manager.historian_vip` says `tns.historian`. The two must agree; neither side has a fallback.

Configuration. The nodes use `db_topic` `tns`, so their archive topics are `tns/archive/<node name>/records`,
`.../definitions` and `.../status`. The historian's config-store entry `config`
(`containers/tent/central/agent_configs/tns-historian.json`) therefore captures `tns/archive/` and nothing else under
`tns/`: the nodes exchange transactive signals on `tns/<sender>/<receiver>`, and those are not records.

```json
{
  "connection": {"params": {"url": "sqlite:////home/vuzer/db/historian.sqlite"}},
  "all_platforms": true,
  "capture_device_data": true,
  "capture_analysis_data": true,
  "capture_log_data": true,
  "capture_record_data": true,
  "custom_topics": {"capture_record_data": ["tns/archive/"]}
}
```

`all_platforms` is required: the nodes run on other platforms of the federation. The URL is a SQLAlchemy URL (four
slashes for an absolute SQLite path); the database lives on the `central_volume` mount.

Installation, outside the containers:

```shell
vctl install-lib /path/to/tns-utilities/lib-archivable
vctl install-lib /path/to/TENT
vctl install /path/to/volttron-transactive-node-historian --vip-identity tns.historian --tag historian --start
vctl config store tns.historian config containers/tent/central/agent_configs/tns-historian.json
```

Registration path. The modular platform has no cross-platform RPC, so nodes on other platforms than the historian use
`"registration": "pubsub"` (the campus sample); a node on the historian's platform may use the default `rpc`.

Authorization for the RPC path (the container setup script does not apply these; run them on the central platform):

```shell
vctl authz add agent tns.historian --protected-rpcs create_tables
vctl authz add agent tn.campus --rpc-capabilities tns.historian.create_tables
vctl authz add agent tn.city --rpc-capabilities tns.historian.create_tables
```

The pubsub path has no per-publisher capability; see the security note under `data_manager` above.

Archive schema version. The historian's TENT and every node's TENT must have the same
`tent.data_manager.ARCHIVE_SCHEMA_VERSION`; the containers build all of them from `code/TENT`. A historian database
written with another version is refused at the historian's start-up and must be recreated (delete the file on the
volume or point the URL elsewhere).

Verifying a deployment. From inside the central platform, the historian's `scripts/probe_queries.py` lists the stored
topics and queries one:

```shell
python scripts/probe_queries.py tcp://172.28.5.6:22916 tns.historian 'Campus/SMALL_OFFICE_DR/*/Power'
```

The TENT analysis tool (`code/tent-analysis`) reads the resulting database directly.

## Tests

The tests run without a platform. `tests/conftest.py` installs fake `volttron` and `gevent` modules when the real ones
are not importable and a fake `Agent` whose `vip` and `core` record what the agent does; the fake pubsub delivers by
the platform's prefix rule. TENT and pytest must be importable.

```shell
python -m pytest
```

The suite covers the data manager, the subscription wrapper, the state-machine loop, configuration NEW and UPDATE, the
meter point, the ILC actuation manager, the weather.gov model and every file in `sample_config/`.
