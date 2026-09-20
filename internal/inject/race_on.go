//go:build race

package inject

// raceEnabled reports whether the test binary was compiled with the race
// detector (-race). Alloc-count assertions are meaningless under race
// instrumentation, which inflates every allocation, so tests that budget
// allocations consult this flag and skip themselves.
const raceEnabled = true
