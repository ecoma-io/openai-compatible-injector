//go:build race

package e2e_test

// raceDetectorOn reports whether the test binary was compiled with the race
// detector (-race). Wall-clock guard tests consult it and skip: race
// instrumentation inflates every timing far past any meaningful budget.
func raceDetectorOn() bool { return true }
