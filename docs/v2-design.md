# Remote Robot Protocol v2

Status: accepted design; implementation proceeds in vertical milestones.

## 1. Problem and goals

The v1 server safely moves one hard-coded SO-101 with one six-value trajectory.
V2 generalizes the system without turning it into arbitrary remote code
execution.

V2 must:

- discover locally registered robots;
- support arbitrary dense control groups rather than a fixed six-axis vector;
- compose physical robots into same-server virtual robots;
- coordinate multiple groups on phase-local timelines and explicit barriers;
- keep interpolation, hardware feedback, failure handling, and safe-stop local;
- pin every executable program to an immutable capability manifest;
- isolate each real robot in a worker process;
- retain structured run evidence after clients disconnect.

V2 doesn't promise cross-server coordination, hard real-time synchronization,
arbitrary uploaded code, unbounded control expressions, or automatic recovery
after server restart.

## 2. Control model

The registry is a directed acyclic graph of `ControlNode` values:

```text
ControlNode
├── PhysicalNode
│   └── ActuatorGroup[]
└── CompositeNode
    └── role -> ControlNode references
```

Only an `ActuatorGroup` receives commands. A group has a fixed ordered axis set,
versioned command schemas, versioned telemetry schemas, and a local control
rate. Samples are dense: every sample supplies every axis in the sequence.

A composite node is a coordination view. It owns no fake flattened vector. It
adds role names, resource acquisition, barriers, fault propagation, and
composite safety policies around its descendants.

The graph may be a DAG so one physical robot can appear in several virtual
robots. Cycles are invalid. At run time the selected root expands to a unique
leaf set and the run atomically leases every referenced leaf and resource.

## 3. Stable identity and discovery

The server loads a declarative TOML registry at startup. Registration is local;
clients cannot upload adapters or modify the registry. Changes take effect only
after restart.

Each exposed root publishes a recursive capability manifest containing:

- `server_id`, `node_id`, kind, revision, and manifest hash;
- child roles and child manifest hashes;
- group IDs, ordered axes, canonical units, and control rate;
- supported command and telemetry schemas;
- a public `configuration_revision` for safety-relevant local configuration;
- resource and safety-domain summaries;
- watchdog and unattended-operation assurances;
- safety-policy revisions and explicit omissions such as
  `cross_collision_checked=false`.

Manifests never expose serial credentials, tokens, filesystem secrets, or other
adapter connection details. Every physical registration must increment its
public `configuration_revision` whenever calibration or another
safety-relevant private connection setting changes. A run pins the exact root
manifest hash. Any child, axis, adapter, configuration revision, role mapping,
or safety-policy change produces a new hash and rejects stale compiled
programs.

Clients call `list_robots()` for authorized exposed roots and
`describe_robot(node_id)` for a full manifest. A local `exposed_as_run_root`
flag decides whether a physical or virtual node may be selected directly.

## 4. Roles, leaves, and programs

Templates address groups through semantic role paths:

```text
left_arm.arm
right_arm.gripper
conveyor.drive
```

Compilation resolves each path to a canonical `(physical_node_id, group_id)`.
The compiled artifact records both representations and the root manifest hash.
Changing a role mapping invalidates the artifact.

Templates are human-readable JSON or TOML. Executable `.rrp` artifacts are
canonical CBOR. Canonical encoding gives each outline, sequence, phase, and full
artifact a deterministic SHA-256 digest without JSON numeric-normalization
ambiguity.

Canonical units are:

- radians for revolute axes;
- metres for prismatic axes;
- dimensionless `[0, 1]` for normalized actuators;
- integer microseconds for trajectory time.

Raw encoder ticks and motor registers never cross the client interface.

## 5. Run, phase, and sequence semantics

A run is one execution lifecycle under one exclusive root lease, from `OPEN`
through a terminal state. It may include many phases and mode transitions.

Before start, the client seals a complete outline listing all phases, role
paths, command schemas, mode transitions, and expected phase digests. The
server uses the outline to resolve and atomically lease the complete set of
leaves and local resources the program will need. A running program cannot add
new phases or participants.

Each phase contains at most one sequence per group. Sequence samples are dense
and use a fixed schema and reference mode. All sequences in a phase share a
phase-local clock beginning at zero.

A phase is the minimum atomic commit unit:

1. receive every sequence payload;
2. verify digests and schemas;
3. resolve relative references from measured phase-start state;
4. compile and apply every leaf/physical/composite safety policy;
5. commit the entire phase or reject it without partial queue mutation;
6. arm every participating worker;
7. start at a shared future monotonic deadline.

`start_phase()` acknowledges scheduling immediately; it never blocks an RPC
until the future deadline. Actual start and completion arrive as events carrying
the `run_id` and a unique `phase_execution_id`. The orchestrator ignores stale
events from earlier runs and computes synchronization error only from matching
`phase_started` events.

Later phase payloads may stream while the current phase executes. At a barrier,
phase time stops. Mode transitions complete and all next-phase workers report
ready before the next phase begins at local time zero. If the next phase isn't
committed by the local deadline, the run safely stops rather than executing
late data.

Mode changes are allowed between sequences only through explicit barriers.
Adapters publish supported schemas and safe transition edges. A client cannot
assume a mode switch is instantaneous.

The initial schemas are dense position trajectories. The schema registry
reserves typed completion predicates and recovery phases, but v2 milestone 1
doesn't implement free branching, loops, expressions, or uploaded predicates.

## 6. Synchronization

All workers belong to one server host. The orchestrator waits until every
participating worker is armed, then sends one future monotonic start deadline.
Workers report actual start time and observed skew.

This is soft synchronization. A composite declares `max_start_skew_us`, and v2
only promises a skew no worse than the slowest participating control period.
Sub-millisecond or safety-critical synchronization requires a shared real-time
controller or field bus outside this Python orchestrator.

## 7. Ownership, resources, and concurrency

Observers may share discovery and telemetry. Control is exclusive over the
program's expanded leaf/resource union. Different runs may execute concurrently
when their leaf sets, exclusive resources, and safety domains don't conflict.

Resources use stable local keys such as `serial:COM3` or `camera:overhead_1`.
Safety domains model overlapping physical workspaces. Acquisition is atomic;
the system never starts half a composite run.

One run targets one selected root. Because a root may be a composite, the same
orchestration implementation handles a physical robot, a workcell, or several
same-server robots. Cross-server composition is out of scope.

## 8. Failure and safety

Any participating leaf failure faults the root run. Every leased leaf performs
its locally registered safe-stop concurrently. Parent policies may make a child
policy stricter but never weaken or disable it.

Policies layer from leaf to physical node to composite node:

```text
leaf: axis/rate/drive-health limits
physical: self-collision, kinematics, workspace
composite: cross-robot collision and shared-zone rules
```

Offline validators run before phase commit. Runtime monitors supervise measured
state, tracking, staleness, current, temperature, worker health, and dynamic
zones where adapters expose those signals.

Each real registration declares a worker-crash watchdog. A drive timeout,
independent controller, safety PLC, or power supervisor may satisfy it. Robots
without an independent watchdog are `supervised_only` and cannot claim
unattended operation.

## 9. Worker and adapter seams

`RunOrchestrator` is the deep Module owning registry selection, leases, phase
state, barriers, failure propagation, event sequencing, and journaling.

The remote-owned worker seam is a small typed interface:

```text
prepare_disabled()
prepare_phase(phase_plan)
start_phase(monotonic_deadline)
stop(fault_context)
state()
close()
```

Production uses a subprocess adapter over length-prefixed canonical CBOR on
local stdio or loopback IPC. Tests use an in-memory worker adapter. Pickle isn't
part of the interface. The IPC contract is language-neutral even though the
first SDK and workers are Python.

Inside a worker, the true-external hardware seam is `RobotAdapter`:

```text
describe_groups()
prepare_disabled()
read_state()
enter_modes(requested_modes)
write_group_commands(commands)
safe_stop(fault_context)
close()
```

Network framing, authentication, program parsing, global leases, and event
journaling never enter a robot adapter.

## 10. Network interfaces

The public protocol is `robot-stream.v2`.

- discovery/status uses a management connection;
- each active run uses its own bidirectional stream;
- telemetry observers use independent streams;
- emergency-stop uses an independent high-priority connection.

The high-level client remains small:

```python
client = await RobotClient.connect(server)
robots = await client.list_robots()
manifest = await client.describe_robot("packing_cell")
program = client.compile(template, manifest)
run = await client.execute(program)
```

Reliable ACK/state/fault/terminal events have monotonically increasing sequence
numbers and can be replayed after reconnect. Telemetry snapshots use latest
value semantics and may be coalesced. A reconnect can recover observation and a
still-valid lease, but whether motion continues is decided locally by heartbeat,
buffer, grace, and sealed-autonomy policies.

A `run_id` is never reused after its first journal event. Retrying before any
lease or journal mutation may reuse the proposed ID; after `run.opened`, callers
must use reliable replay rather than starting another lifecycle under the same
identity.

Authorization is identity- and scope-based: discover, observe, control, and
emergency-stop are distinct per-root permissions. A first deployment may have
one full-access identity, but the implementation doesn't equate authentication
with universal access.

## 11. Run journal

The server persists an immutable run journal containing identity, root and
child manifest hashes, artifact digest, role resolution, resource leases,
phase watermarks, actual phase starts, measured skew, transitions, faults,
safe-stop actions, and the digest of any live-resolved relative trajectory.

Secrets and raw credentials are never journalled. Telemetry retention is
bounded and configurable. Journals support diagnosis only; a restart never
automatically resumes an old run or re-enables torque.

## 12. SO-101 migration

The v2 SO-101 physical node exposes:

- `arm`: five revolute axes in radians;
- `gripper`: one normalized opening axis in `[0, 1]`.

Its adapter may still combine both groups into one Feetech sync transaction.
Calibration-derived travel limits and separate body/gripper supervision from
the v1 bug-fix remain authoritative.

V2 intentionally breaks v1 wire compatibility. The v1 baseline is retained in
Git history. A migration command converts old six-value degree trajectories to
a single-phase v2 template/compiled artifact bound to a selected SO-101
manifest.

## 13. Implementation milestones

1. **Capability foundation** — TOML registry, ControlNode DAG, recursive
   manifests/hashes, identity-filtered discovery, arbitrary dense group specs.
2. **Program foundation** — JSON/TOML templates, role resolution, phase outline,
   canonical CBOR artifact, schema validation, inspect/validate tooling.
3. **Execution foundation** — atomic lease manager, run journal, in-memory
   simulator workers, multi-group phase/barrier execution.
4. **Isolation** — language-neutral CBOR worker IPC, subprocess supervisor,
   crash and timeout injection.
5. **Network v2** — discovery and per-run WebSockets, phase streaming, reliable
   event replay, scoped identities.
6. **Hardware migration** — SO-101 split groups and adapter, calibration policy,
   supervised-only watchdog declaration, v1 converter.

Every milestone is verified through the same deep Module interfaces used by
callers. Tests replace production worker/network adapters rather than reaching
past those interfaces into internal queues.

## 14. Current implementation boundary

Implemented in the first vertical slice:

- registry DAG, recursive manifests, public configuration revision, and scoped
  discovery;
- arbitrary-dimensional dense program compilation and deterministic `.rrp`
  artifacts, including reconstruction against the current registry before
  execution;
- whole-outline leases, immutable journals, correlated worker events, common
  start deadlines, skew enforcement, barriers, and coordinated safe-stop;
- in-memory and subprocess simulator workers over canonical CBOR IPC;
- authenticated management WebSocket discovery and a v1 SO-101 converter;
- a loopback-only, read-only operations projection and HTTP/SSE dashboard for
  registry topology, worker reachability, leases, runs, journal events, and
  declared safety posture.

Deliberately not exposed yet:

- the public per-run streaming WebSocket and telemetry/estop streams;
- the real v2 SO-101 adapter and phase-start measured-state resolution;
- physical/composite safety validators beyond the declared registry policies;
- non-empty mode transitions, custom completion predicates, recovery phases,
  and restart-time execution resume.

Until those seams are implemented and tested, the v2 server reports
`execution_available=false`; only the existing v1 server may touch the real
SO-101 hardware.
