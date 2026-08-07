# V2 server/client example

This directory is a runnable, simulator-only example of the currently exposed
Remote Robot Protocol v2 surface:

```text
server/registry/                  server-local registration
  server.toml
  robots/demo_robot.toml          physical robot: 3-axis arm + 1-axis gripper
  robots/demo_cell.toml           virtual robot exposing role `robot`
  identities/example_operator.toml
client/
  program.json                    two dense groups on one phase clock
  discover_and_compile.py         discover manifest and compile canonical CBOR
```

No file in this example opens a serial port or enables torque.
Both sides use the local loopback interface: the server binds only
`127.0.0.1`, and the client connects to `ws://127.0.0.1:8766`. Nothing in this
example is exposed to the LAN or Internet.

## 1. Install

From the repository root:

```powershell
uv sync
```

## 2. Start the server

Create a random token. Keep the value outside source control:

```powershell
$env:REMOTE_ROBOT_EXAMPLE_TOKEN = uv run python -c "import secrets; print(secrets.token_urlsafe(32))"
$env:REMOTE_ROBOT_EXAMPLE_TOKEN
```

Start the v2 management server:

```powershell
uv run python -m remote_robot.server_v2 `
  --registry examples/server/registry `
  --host 127.0.0.1 `
  --port 8766 `
  --dashboard-host 127.0.0.1 `
  --dashboard-port 8080
```

Open `http://127.0.0.1:8080` on the same computer to inspect the read-only
operations dashboard. This example intentionally has no hardware worker, so
the physical node appears as offline while its registration, hierarchy,
groups, and safety declarations remain visible. The browser cannot submit
motion commands.

The server loads registration only at startup. Edit the TOML files and restart
the process to apply changes. Increment a physical node's
`configuration_revision` whenever a safety-relevant private configuration or
calibration changes.

## 3. Run the client

Open another PowerShell window, set the same token, then run:

```powershell
$env:REMOTE_ROBOT_EXAMPLE_TOKEN = "paste-the-server-token-here"
uv run python examples/client/discover_and_compile.py
```

The client performs these operations:

1. authenticates as `example_operator`;
2. calls `server_info()` and `list_robots()`;
3. downloads the recursive manifest for `demo_cell`;
4. resolves `robot.arm` and `robot.gripper` locally;
5. compiles `program.json` into deterministic `program.rrp` canonical CBOR.

You can override the defaults:

```powershell
uv run python examples/client/discover_and_compile.py `
  --url ws://127.0.0.1:8766 `
  --identity example_operator `
  --token-env REMOTE_ROBOT_EXAMPLE_TOKEN `
  --root demo_cell `
  --program examples/client/program.json `
  --output examples/client/program.rrp
```

The current v2 network service deliberately reports
`execution_available=false`. Discovery and compilation are real and runnable;
uploading/executing the resulting `.rrp` over the network is deferred until the
per-run stream is connected to `RunOrchestrator`.
