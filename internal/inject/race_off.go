//go:build !race

package inject

// raceEnabled mirrors the build-tagged value of race_on.go: false unless the
// binary was compiled with -race, in which case the race-tagged definition
// wins. Allocation-budget tests consult it and skip under -race, because
// race instrumentation inflates alloc counts.
const raceEnabled = false
