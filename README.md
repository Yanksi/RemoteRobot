# Remote robot trajectory streaming

The stable v1 path still controls one SO-101.  A v2 implementation is being
built alongside it to support locally registered physical and virtual robots,
hierarchical control groups, arbitrary group dimensions, and coordinated
multi-group phases.  The accepted architecture and its safety boundaries are
recorded in [`docs/v2-design.md`](docs/v2-design.md).

This project keeps every time-sensitive operation on the computer physically
connected to the SO-101.  The remote computer sends timestamped joint
trajectory chunks over one bidirectional WebSocket; the Canada-side server
validates, buffers, interpolates, executes, and streams feedback.

The protocol never accepts Python, shell commands, raw serial packets, motor
register writes, or torque parameters.

## Architecture

```text
trajectory file -> robot_client.py == ZeroTier/WebSocket ==> robot_server.py
                                                          -> bounded buffer
                                                          -> 20 Hz local loop
                                                          -> Feetech serial bus
                              <== ACK / cursor / q / error ==
```

An ACK means that the entire chunk passed validation and was atomically added
to the server buffer.  It does **not** mean that the arm executed it.  Physical
progress is reported by `execution_cursor_us`, telemetry, and `run_finished`.

## Install

Python 3.12 and uv are used because the robot dependencies don't currently
build cleanly with this project on Python 3.13.

```powershell
uv sync
```

Create the same random token on both machines.  Don't put it in source control:

```powershell
$env:ROBOT_SERVER_TOKEN = python -c "import secrets; print(secrets.token_urlsafe(32))"
$env:ROBOT_SERVER_TOKEN
```

Transfer that value to the other machine through a secure channel and set the
same environment variable there.

## Canada-side server

First use the simulator.  Bind specifically to the Canada computer's ZeroTier
address rather than `0.0.0.0`:

```powershell
uv run python robot_server.py --backend sim --host 10.x.x.x
```

Only after simulation, local workspace checks, a physical E-stop, and a person
standing by at the arm should the real backend be used:

```powershell
uv run python robot_server.py `
  --backend so101 `
  --host 10.x.x.x `
  --serial-port COM3 `
  --robot-id my_follower_arm `
  --calibration-dir calibration
```

The server refuses automatic calibration.  It also connects with torque off,
reads the current positions, overwrites stale goal registers with those values,
validates the start pose, and only then enables torque.

## Client

Validate a program completely offline:

```powershell
uv run python robot_client.py validate programs/relative_demo.rmp.jsonl
```

`relative_demo` is a protocol/simulator fixture, not a collision-certified
motion for a real workspace.  Joint limits alone cannot prove clearance from a
table, fixture, cable, or person.

Run in `sealed` mode (upload and validate the entire file before torque):

```powershell
uv run python robot_client.py --url ws://10.x.x.x:8765 run `
  programs/relative_demo.rmp.jsonl --mode sealed
```

Run a long program in streaming mode.  The client stays about five seconds
ahead while the server performs the local loop:

```powershell
uv run python robot_client.py --url ws://10.x.x.x:8765 run `
  path.rmp.jsonl --mode streaming --target-ahead 5
```

Status and independent stop requests use new WebSocket connections:

```powershell
uv run python robot_client.py --url ws://10.x.x.x:8765 status
uv run python robot_client.py --url ws://10.x.x.x:8765 stop
```

`stop` is still a best-effort network command.  It cannot replace a Canada-side
physical power cut-off.

## Program format

Files are versioned NDJSON: one header followed by timestamped, complete six
joint vectors.  Times are integer microseconds relative to the run start; joint
coordinates are calibrated degrees in the canonical order.

```jsonl
{"type":"header","format":"so101-joint-trajectory","version":1,"program_id":"demo","model":"so101-follower","joints":["shoulder_pan","shoulder_lift","elbow_flex","wrist_flex","wrist_roll","gripper"],"coordinate_mode":"relative_deg","interpolation":"quintic_stop"}
{"type":"point","t_us":0,"q_deg":[0,0,0,0,0,0]}
{"type":"point","t_us":2000000,"q_deg":[2,0,0,0,0,0]}
{"type":"point","t_us":4000000,"q_deg":[0,0,0,0,0,0]}
```

`relative_deg` anchors the all-zero first point to the measured pose.  The
server resolves and revalidates every later point against its absolute limits
before enabling torque.  `absolute_deg` is more deterministic, but its first
point must already be within the server's start-pose tolerance.

Every segment uses a deterministic quintic profile with zero velocity and
acceleration at both endpoints.  The server checks its exact peak velocity and
acceleration before accepting the chunk.

## Execution modes and failure behavior

- `sealed`: all points and the SHA-256 digest must validate before start.  This
  is the recommended default and doesn't need WAN data during execution.
- `streaming`: start after at least two seconds are buffered, accept chunks only
  inside a 30-second horizon, and report low water below one second.
- Missing heartbeat, buffer underrun, persistent tracking error, serial failure,
  or a local loop overrun terminates the run through the same hold-then-disable
  path.  An underrun never extrapolates the last velocity and never auto-resumes.
- `--disconnect-policy complete_if_sealed` is an explicit opt-in for a sealed
  job to finish after the client disappears.  The default is to stop after the
  heartbeat timeout.

ZeroTier encrypts the link.  For defense in depth, a later deployment can put
the WebSocket behind a TLS reverse proxy and use `wss://`; the application token
is still required.

## Tests

Tests don't touch COM3 or enable real torque:

```powershell
uv run python -m unittest discover -s tests -v
```

## V2 development snapshot

V2 registration is declarative and server-local.  The sample registry contains
a split SO-101 (`arm` with five revolute axes and `gripper` with one normalized
axis), a seven-axis simulator, and a virtual workcell containing both:

```powershell
uv run python -m remote_robot.cli --registry registry.example list `
  --identity workcell_operator
uv run python -m remote_robot.cli --registry registry.example describe `
  demo_workcell
uv run python -m remote_robot.cli --registry registry.example compile `
  programs/v2_workcell_demo.json programs/v2_workcell_demo.rrp
uv run python -m remote_robot.cli --registry registry.example validate `
  programs/v2_workcell_demo.rrp
```

The `.rrp` file is deterministic canonical CBOR pinned to the complete root
manifest.  Each sequence targets a semantic role path such as
`primary_arm.arm`; compilation resolves it to a canonical physical group.  A
phase may contain several dense sequences of different dimensions on one
shared phase-local clock.

The current network entry point exposes authenticated, identity-filtered
discovery only.  Set the credential named by the selected identity TOML before
starting it:

```powershell
$env:REMOTE_ROBOT_WORKCELL_TOKEN = "replace-with-at-least-32-random-characters"
uv run python -m remote_robot.server_v2 --registry registry.example

# In another shell with the same credential:
uv run python -m remote_robot.client_v2 `
  --identity workcell_operator `
  --token-env REMOTE_ROBOT_WORKCELL_TOKEN list
```

The same process also exposes a read-only operations dashboard at
`http://127.0.0.1:8080`. It shows the registered hierarchy, group dimensions,
worker reachability, active leases, run state, journal events, and declared
safety posture. The dashboard is deliberately restricted to a loopback IP and
has no motion-command endpoint. Pass `--dashboard-port 0` to disable it.

The local `RunOrchestrator` already executes multi-worker simulator phases
through atomic leases, correlated scheduled/start/completion events,
append-only run journals, and length-prefixed canonical-CBOR subprocess IPC.
It coordinates safe-stop and releases the complete resource union on any
failure.  The v2 server nevertheless reports network execution as unavailable
until the run-stream endpoint and a real v2 hardware adapter are wired to that
orchestrator; it never silently falls back to the v1 six-axis protocol.
