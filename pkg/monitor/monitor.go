/*
Copyright 2026 Intel Corporation

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package monitor

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
)

const (
	defaultResctrlRoot = "/sys/fs/resctrl"
	monGroupsDir       = "mon_groups"
	tasksFile          = "tasks"
)

// Typed errors returned by Manager methods.
var (
	// ErrNotTracked is returned when an operation references a key that has
	// no active mon_group.
	ErrNotTracked = errors.New("monitor: key not tracked")

	// ErrNoRMIDs is returned when the kernel has no available RMIDs (mkdir
	// returns ENOSPC on the resctrl filesystem).
	ErrNoRMIDs = errors.New("monitor: no RMIDs available")

	// ErrBadKey is returned when a key fails the configured KeyValidator.
	ErrBadKey = errors.New("monitor: invalid key")

	// ErrBadClass is returned when an rdtClass name is unsafe (path
	// traversal, empty, or contains separators).
	ErrBadClass = errors.New("monitor: invalid rdt class")
)

var log *slog.Logger = slog.Default()

// SetLogger sets the logger used by the package. Safe to call before New.
func SetLogger(l *slog.Logger) { log = l }

// Options configures a Manager.
type Options struct {
	// ResctrlRoot is the resctrl mount point. Default: "/sys/fs/resctrl".
	// Tests typically set this to a temp directory.
	ResctrlRoot string

	// KeyValidator, if set, rejects keys that do not satisfy it (e.g. the
	// pod-UID UUID shape). Default: accept any non-empty key without path
	// separators or NUL bytes.
	//
	// The validator also scopes Reconcile: only on-disk mon_group directories
	// whose name satisfies KeyValidator are eligible for orphan removal, so a
	// pod-UID validator confines reaping to pod-shaped directories and never
	// touches groups created by other tools.
	KeyValidator func(key string) bool

	// KeyCanonicalizer, if set, normalizes a caller key to a canonical form
	// before it is used as a mon_group directory name and as the in-memory
	// tracking key. It is applied after KeyValidator. Default: identity.
	//
	// Pair it with a matching KeyValidator (e.g. CanonicalizePodUID alongside
	// PodUIDValidator) so that keys reported in different-but-equivalent forms
	// (e.g. a pod UID with or without dashes) map to a single, predictable
	// on-disk directory name.
	KeyCanonicalizer func(key string) string
}

// Manager owns the lifecycle of per-workload resctrl mon_groups.
//
// It is safe for concurrent use from multiple goroutines.
type Manager struct {
	root     string
	validKey func(string) bool
	canonKey func(string) string

	mu      sync.Mutex
	entries map[string]*entry // keyed by caller key (e.g. pod UID)

	// Injectable filesystem operations for unit tests.
	mkdir func(string, os.FileMode) error
	rmdir func(string) error
}

// entry is the in-memory record for one tracked key.
type entry struct {
	dir      string              // absolute mon_group directory path
	members  map[string]struct{} // member IDs (e.g. container IDs)
	rdtClass string              // rdtClass used when group was created
}

// Group is a handle to one mon_group on the resctrl filesystem.
type Group struct {
	key string
	dir string
}

// Key returns the caller-provided key (e.g. pod UID) for this group.
func (g *Group) Key() string { return g.key }

// Path returns the absolute filesystem path of the mon_group directory.
func (g *Group) Path() string { return g.dir }

// New creates a Manager with the given options.
func New(o Options) (*Manager, error) {
	root := o.ResctrlRoot
	if root == "" {
		root = defaultResctrlRoot
	}
	valid := o.KeyValidator
	if valid == nil {
		valid = DefaultKeyValidator
	}
	canon := o.KeyCanonicalizer
	if canon == nil {
		canon = func(k string) string { return k }
	}
	return &Manager{
		root:     root,
		validKey: valid,
		canonKey: canon,
		entries:  make(map[string]*entry),
		mkdir:    os.Mkdir,
		rmdir:    os.Remove,
	}, nil
}

// EnsureGroup idempotently creates a mon_group for key under an optional
// pre-existing rdtClass ctrl_group ("" = root resctrl directory). It never
// creates the ctrl_group itself. Returns a Group handle on success.
func (m *Manager) EnsureGroup(key, rdtClass string) (*Group, error) {
	if !m.validKey(key) {
		return nil, fmt.Errorf("%w: %q", ErrBadKey, key)
	}
	if rdtClass != "" && !isValidRDTClass(rdtClass) {
		return nil, fmt.Errorf("%w: %q", ErrBadClass, rdtClass)
	}
	key = m.canonKey(key)

	// Defense-in-depth: verify the canonicalized key is still a safe single
	// path component. A buggy KeyCanonicalizer could introduce slashes or
	// dot-segments that would escape the mon_groups directory.
	if !DefaultKeyValidator(key) {
		return nil, fmt.Errorf("%w: canonicalized %q is not path-safe", ErrBadKey, key)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// Fast path: already tracked — return existing group, no syscall.
	if e, ok := m.entries[key]; ok {
		if rdtClass != e.rdtClass {
			return nil, fmt.Errorf("key %q already tracked under rdtClass %q, cannot reassign to %q", key, e.rdtClass, rdtClass)
		}
		return &Group{key: key, dir: e.dir}, nil
	}

	// Determine parent directory.
	parentDir := m.root
	if rdtClass != "" {
		parentDir = filepath.Join(m.root, rdtClass)
		// The ctrl_group must already exist (created by an allocation plugin).
		info, err := os.Stat(parentDir)
		if err != nil {
			return nil, fmt.Errorf("ctrl_group %s does not exist: %w", parentDir, err)
		}
		if !info.IsDir() {
			return nil, fmt.Errorf("ctrl_group %s is not a directory", parentDir)
		}
	}

	monGroupsPath := filepath.Join(parentDir, monGroupsDir)
	monGroupDir := filepath.Join(monGroupsPath, key)

	// Ensure the mon_groups/ directory exists. On a real resctrl mount this
	// is always present; for testing we create it if needed.
	if err := os.MkdirAll(monGroupsPath, 0755); err != nil {
		return nil, fmt.Errorf("mon_groups dir not available at %s: %w", monGroupsPath, err)
	}

	// Use Mkdir (not MkdirAll) for the final mon_group directory to avoid
	// accidentally creating a ctrl_group if rdtClass is wrong.
	if err := m.mkdir(monGroupDir, 0755); err != nil {
		if errors.Is(err, os.ErrExist) {
			// Already on disk (e.g. from a previous run) — adopt it.
			log.Info("adopting existing mon_group", "key", key, "dir", monGroupDir)
		} else if errors.Is(err, syscall.ENOSPC) {
			return nil, fmt.Errorf("%w (key %s): %w", ErrNoRMIDs, key, err)
		} else {
			return nil, fmt.Errorf("failed to create mon_group %s: %w", monGroupDir, err)
		}
	} else {
		log.Info("created mon_group", "key", key, "dir", monGroupDir)
	}

	m.entries[key] = &entry{
		dir:      monGroupDir,
		members:  make(map[string]struct{}),
		rdtClass: rdtClass,
	}
	return &Group{key: key, dir: monGroupDir}, nil
}

// AssignPID writes pid to the group's tasks file. The kernel assigns the RMID
// to this PID and all future child processes. Call while the init process is
// created but paused (the pre-fork window) for race-free attribution.
func (m *Manager) AssignPID(key string, pid int) error {
	if pid <= 0 {
		return fmt.Errorf("invalid pid %d: must be positive", pid)
	}
	key = m.canonKey(key)
	m.mu.Lock()
	e, ok := m.entries[key]
	m.mu.Unlock()
	if !ok {
		return fmt.Errorf("%w: %q", ErrNotTracked, key)
	}

	tasksPath := filepath.Join(e.dir, tasksFile)
	f, err := os.OpenFile(tasksPath, os.O_WRONLY, 0)
	if err != nil {
		return fmt.Errorf("failed to open tasks file for key %s: %w", key, err)
	}
	defer f.Close()

	data := []byte(strconv.Itoa(pid) + "\n")
	if _, err := f.Write(data); err != nil {
		return fmt.Errorf("failed to write pid %d for key %s: %w", pid, key, err)
	}
	log.Info("assigned PID to mon_group", "key", key, "pid", pid)
	return nil
}

// Remove deletes the mon_group for key (kernel releases the RMID) and drops
// all in-memory state for that key. Removing a directory that is already gone
// on disk is not an error, but calling Remove for a key that is not tracked
// returns ErrNotTracked.
func (m *Manager) Remove(key string) error {
	key = m.canonKey(key)
	m.mu.Lock()
	e, ok := m.entries[key]
	if !ok {
		m.mu.Unlock()
		return fmt.Errorf("%w: %q", ErrNotTracked, key)
	}
	m.mu.Unlock()

	if err := m.rmdir(e.dir); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("failed to remove mon_group %s: %w", e.dir, err)
	}

	// Only drop the in-memory entry after successful on-disk removal.
	m.mu.Lock()
	delete(m.entries, key)
	m.mu.Unlock()

	log.Info("removed mon_group", "key", key, "dir", e.dir)
	return nil
}

// AddMember registers a member ID (e.g. container ID) under an existing key.
// It is a no-op if key is not tracked.
func (m *Manager) AddMember(key, memberID string) {
	key = m.canonKey(key)
	m.mu.Lock()
	defer m.mu.Unlock()
	if e, ok := m.entries[key]; ok {
		e.members[memberID] = struct{}{}
	}
}

// RemoveMember unregisters a member ID from a key.
// It is a no-op if key or memberID is not tracked.
func (m *Manager) RemoveMember(key, memberID string) {
	key = m.canonKey(key)
	m.mu.Lock()
	defer m.mu.Unlock()
	if e, ok := m.entries[key]; ok {
		delete(e.members, memberID)
	}
}

// MemberCount returns the number of members tracked for key, or 0 if the key
// is not tracked.
func (m *Manager) MemberCount(key string) int {
	key = m.canonKey(key)
	m.mu.Lock()
	defer m.mu.Unlock()
	if e, ok := m.entries[key]; ok {
		return len(e.members)
	}
	return 0
}

// Reconcile removes on-disk mon_groups whose key is not present in live. Only
// directories whose name satisfies the Manager's KeyValidator are eligible, so
// a pod-UID validator confines reaping to pod-shaped directories and never
// touches groups created by other tools or kernel metadata.
//
// Note: Reconcile identifies groups by their directory name (the key). If the
// same key were to appear under multiple ctrl_groups due to misconfiguration,
// only the in-memory tracked instance is authoritative; duplicates under other
// ctrl_groups are treated as orphans and removed.
func (m *Manager) Reconcile(live []string) error {
	liveSet := make(map[string]struct{}, len(live))
	for _, k := range live {
		liveSet[m.canonKey(k)] = struct{}{}
	}

	// Scan root-level mon_groups.
	m.reconcileDir(filepath.Join(m.root, monGroupsDir), liveSet)

	// Scan ctrl_group-level mon_groups.
	entries, err := os.ReadDir(m.root)
	if err != nil {
		return fmt.Errorf("reconcile: failed to read resctrl root %s: %w", m.root, err)
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		name := entry.Name()
		// Skip non-ctrl_group directories.
		if name == monGroupsDir || name == "info" || strings.HasPrefix(name, "mon_") {
			continue
		}
		m.reconcileDir(filepath.Join(m.root, name, monGroupsDir), liveSet)
	}
	return nil
}

// reconcileDir scans a single mon_groups/ directory and removes entries whose
// name satisfies the Manager's KeyValidator but is not in liveSet.
func (m *Manager) reconcileDir(monGroupsPath string, liveSet map[string]struct{}) {
	entries, err := os.ReadDir(monGroupsPath)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			log.Warn("reconcile: failed to read mon_groups dir", "path", monGroupsPath, "err", err)
		}
		return
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		name := entry.Name()

		// Skip kernel-created metadata directories inside mon_groups/.
		if name == "info" || strings.HasPrefix(name, "mon_") {
			continue
		}

		// Only consider directories whose name passes validation (e.g. the
		// pod-UID shape), which scopes reaping to groups this mechanism owns.
		if !m.validKey(name) {
			continue
		}

		// Canonicalize the on-disk name before lookup so it matches the
		// canonicalized keys in liveSet (e.g. case-folding, dash insertion).
		canon := m.canonKey(name)
		if _, ok := liveSet[canon]; ok {
			continue
		}

		// Orphan — remove it.
		orphanDir := filepath.Join(monGroupsPath, name)
		if err := m.rmdir(orphanDir); err != nil && !errors.Is(err, os.ErrNotExist) {
			log.Warn("reconcile: failed to remove orphan", "dir", orphanDir, "err", err)
			continue
		}
		log.Info("reconcile: removed orphan mon_group", "key", name, "dir", orphanDir)

		// Drop from in-memory state only if the tracked path matches the
		// orphan we just removed.  A key can legitimately exist under a
		// different ctrl_group (e.g. /BestEffort/mon_groups/<key>) while an
		// orphan copy lingers at the root level.
		m.mu.Lock()
		if e, ok := m.entries[name]; ok && e.dir == orphanDir {
			delete(m.entries, name)
		}
		m.mu.Unlock()
	}
}

// List returns the keys currently tracked in memory.
func (m *Manager) List() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	keys := make([]string, 0, len(m.entries))
	for k := range m.entries {
		keys = append(keys, k)
	}
	return keys
}

// Snapshot returns a point-in-time map of tracked key -> *Group handle. The
// returned Group values are copies safe to use after the lock is released.
func (m *Manager) Snapshot() map[string]*Group {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[string]*Group, len(m.entries))
	for k, e := range m.entries {
		out[k] = &Group{key: k, dir: e.dir}
	}
	return out
}
