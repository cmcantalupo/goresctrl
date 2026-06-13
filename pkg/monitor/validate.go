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
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// DefaultKeyValidator accepts any non-empty key that does not contain path
// separators, NUL bytes, or dot-segments ("." and "..").
func DefaultKeyValidator(key string) bool {
	if key == "" || key == "." || key == ".." {
		return false
	}
	for _, c := range key {
		if c == '/' || c == 0 {
			return false
		}
	}
	return true
}

// PodUIDValidator matches the Kubernetes pod-UID UUID shape in either of the
// two forms a container runtime may report:
//
//   - the standard 8-4-4-4-12 dashed form, e.g.
//     "a1b2c3d4-e5f6-7890-abcd-ef1234567890" (containerd), and
//   - the compact 32-character hex form without dashes, e.g.
//     "a1b2c3d4e5f67890abcdef1234567890" (some CRI-O versions).
//
// Hex digits of either case are accepted. Pair this validator with
// CanonicalizePodUID (via Options.KeyCanonicalizer) so the on-disk mon_group
// directory name is always the canonical dashed form regardless of which form
// the runtime reported.
func PodUIDValidator(key string) bool {
	switch len(key) {
	case 36:
		parts := strings.Split(key, "-")
		if len(parts) != 5 {
			return false
		}
		expectedLens := [5]int{8, 4, 4, 4, 12}
		for i, part := range parts {
			if len(part) != expectedLens[i] {
				return false
			}
			if !isHex(part) {
				return false
			}
		}
		return true
	case 32:
		return isHex(key)
	default:
		return false
	}
}

// CanonicalizePodUID converts a pod UID to the canonical 8-4-4-4-12 lowercase
// UUID form. A compact 32-character hex input has dashes inserted; a 36-
// character input is lowercased and returned unchanged in shape. Any other
// input is returned lowercased but otherwise untouched, so it is safe to use
// as a KeyCanonicalizer guarded by PodUIDValidator. Suitable for use as
// Options.KeyCanonicalizer alongside PodUIDValidator.
func CanonicalizePodUID(key string) string {
	key = strings.ToLower(key)
	if len(key) != 32 || !isHex(key) {
		return key
	}
	// Insert dashes at the 8-4-4-4-12 boundaries.
	return key[0:8] + "-" + key[8:12] + "-" + key[12:16] + "-" + key[16:20] + "-" + key[20:32]
}

// isHex reports whether s consists solely of hexadecimal digits (either case).
func isHex(s string) bool {
	for _, c := range s {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
			return false
		}
	}
	return true
}

// isValidRDTClass returns true if name is a safe resctrl ctrl_group name.
// It rejects path separators, dot-segments, and empty strings to prevent
// path traversal outside the resctrl mount.
func isValidRDTClass(name string) bool {
	if name == "" || name == "." || name == ".." {
		return false
	}
	for _, c := range name {
		if c == '/' || c == 0 {
			return false
		}
	}
	return true
}

// Validate checks that the resctrl filesystem at root is mounted and has
// monitoring support. It returns the list of counter names available in the
// root mon_data directory. An error is returned if the path does not exist
// or has no mon_data subdirectory.
func Validate(root string) ([]string, error) {
	monDataPath := filepath.Join(root, "mon_data")
	domains, err := os.ReadDir(monDataPath)
	if err != nil {
		return nil, fmt.Errorf("resctrl mon_data not available at %s: %w", monDataPath, err)
	}
	seen := make(map[string]struct{})
	for _, d := range domains {
		if !d.IsDir() {
			continue
		}
		files, err := os.ReadDir(filepath.Join(monDataPath, d.Name()))
		if err != nil {
			continue
		}
		for _, f := range files {
			if !f.IsDir() {
				seen[f.Name()] = struct{}{}
			}
		}
	}
	if len(seen) == 0 {
		return nil, fmt.Errorf("no counters found in %s", monDataPath)
	}
	names := make([]string, 0, len(seen))
	for name := range seen {
		names = append(names, name)
	}
	return names, nil
}
