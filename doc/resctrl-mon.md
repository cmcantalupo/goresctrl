# resctrl-mon: Per-Workload Memory Bandwidth & Cache Monitoring

`pkg/monitor` provides a runtime-agnostic library for managing resctrl
`mon_group` directories — one per workload (pod or container) — so that
hardware monitoring counters (MBM, CMT, Intel AET) can be attributed to
individual workloads by downstream tools such as [Kepler](https://sustainable-computing.io/).

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                  pkg/monitor (neutral core)             │
│  Manager: EnsureGroup / AssignPID / Remove / Reconcile  │
└──────────────────────┬──────────────────────────────────┘
                       │
                       ▼
              NRI Plugin (K8s)
           containerd / CRI-O
              Key: pod UID
```

The NRI plugin is the sole supported consumer of `pkg/monitor` for Kubernetes.
It imports only `pkg/monitor`, which knows nothing about NRI or Kubernetes.

## Supported Runtimes

| Runtime | Adapter | Key Source | Mechanism |
|---------|---------|-----------|-----------|
| containerd (K8s, k3s) | NRI plugin | Pod UID from NRI API | NRI stub callbacks |
| CRI-O ≥ 1.36 (OpenShift, K8s) | NRI plugin | Pod UID from NRI API | NRI stub callbacks |

CRI-O ≥ 1.36 is required because earlier versions do not populate the
container PID in the `StartContainer` NRI hook, making race-free RMID
assignment impossible. In practice this is not a constraint: CRI-O 1.36
is already available in current distribution release trains, well ahead
of the kernel AET support (≥ 7.0) that this tooling targets.

## The "One Plugin Per Node" Rule

**Deploy the NRI plugin once per node.** The plugin works identically on
containerd and CRI-O ≥ 1.36 — no per-runtime adapter selection is needed.

## Group Naming and Reconcile Scoping

Each mon_group directory is named by its bare key (pod UID):

- `mon_groups/<pod-uid>`

This matches the convention used by downstream consumers (e.g.
`resctrl_node_exporter`), which read the directory name verbatim as the
workload identifier — no prefix to strip.

`Manager.Reconcile()` is scoped by the configured `KeyValidator`: only
directories whose name satisfies the validator are eligible for orphan
removal. The NRI plugin uses `PodUIDValidator`, so reconcile only ever
considers UUID-shaped directories and never touches groups created by other
tools or kernel metadata directories (`info`, `mon_*`).

`PodUIDValidator` accepts a pod UID in either the standard dashed
`8-4-4-4-12` form (reported by containerd) or the compact 32-character
hex form (reported by some CRI-O versions). Pairing it with
`CanonicalizePodUID` via `Options.KeyCanonicalizer` makes the manager insert
dashes for compact UIDs, so the on-disk `mon_groups/<uid>` directory name is
always the canonical dashed UUID regardless of which runtime created it.

## Quick Start

### Kubernetes: containerd or CRI-O ≥ 1.36 (NRI plugin)

```bash
# Build and deploy the NRI plugin
# (from the nri-plugins repository, resctrl-mon-goresctrl branch)
go build -o resctrl-mon ./cmd/plugins/resctrl-mon/
```

The plugin registers for `PostCreateContainer`, `StartContainer`,
`PostStartContainer`, `StopContainer`, and `Synchronize` events.
Works identically on both containerd and CRI-O ≥ 1.36.

## Library API

```go
import "github.com/intel/goresctrl/pkg/monitor"

// Validate: check resctrl is available and discover supported counters.
counters, err := monitor.Validate("/sys/fs/resctrl")
// counters: ["llc_occupancy", "mbm_total_bytes", "core_energy", ...]

mgr, _ := monitor.New(monitor.Options{
    ResctrlRoot:      "/sys/fs/resctrl",        // default
    KeyValidator:     monitor.PodUIDValidator,   // or nil for standalone
    KeyCanonicalizer: monitor.CanonicalizePodUID, // dash-normalize pod UIDs
})

// Create mon_group (idempotent)
grp, _ := mgr.EnsureGroup(podUID, rdtClass)

// Track container membership (for multi-container pods)
mgr.AddMember(podUID, containerID)

// Assign PID (pre-fork window for race-free attribution)
mgr.AssignPID(podUID, pid)

// On container stop: decrement members
mgr.RemoveMember(podUID, containerID)
if mgr.MemberCount(podUID) == 0 {
    mgr.Remove(podUID)  // deletes directory, releases RMID
}

// Crash recovery: remove orphaned groups not in the live set
mgr.Reconcile(liveKeys)

// Read counters with typed semantics
readings, _ := mgr.ReadCounters(podUID)
for _, r := range readings {
    // r.Kind: monitor.Gauge (instantaneous) or monitor.Cumulative (monotonic counter)
    // r.Unit: "bytes", "joules", "farads", or "" if unknown
    fmt.Printf("%s/%s = %f (%v, %s)\n", r.Domain, r.Name, r.Value, r.Kind, r.Unit)
}
```

## RMID Exhaustion

Each mon_group consumes one RMID from a limited hardware pool (typically
~300-2000 depending on platform). When RMIDs are exhausted, `EnsureGroup`
returns `monitor.ErrNoRMIDs` (wrapping `ENOSPC`). The adapters log this as a
warning and do **not** fail the container start — monitoring degrades
gracefully.

Check available RMIDs:
```bash
cat /sys/fs/resctrl/info/L3_MON/num_rmids
find /sys/fs/resctrl -name tasks -path '*/mon_groups/*' | wc -l
```

## Requirements

- Linux kernel ≥ 7.0 with `CONFIG_X86_CPU_RESCTRL=y` (for AET counters)
- resctrl mounted at `/sys/fs/resctrl` (default on modern kernels)
- containerd ≥ 1.7.0 or CRI-O ≥ 1.36 with NRI enabled
- CRI-O < 1.36 is **not supported** (missing PID in StartContainer hook
  makes race-free RMID assignment impossible)

## Related Packages

- `pkg/rdt`: Config-driven resctrl allocation (ctrl_groups, schemata).
  `pkg/monitor` creates mon_groups *under* ctrl_groups managed by `pkg/rdt`
  but does not depend on `pkg/rdt`'s config machinery.
- `pkg/path`: Shared resctrl path helpers used by both packages.
