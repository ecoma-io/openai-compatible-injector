//go:build !race

package e2e_test

// raceDetectorOn mirrors the build-tagged value of race_on.go: false unless
// the binary was compiled with -race, in which case the race-tagged
// definition wins.
func raceDetectorOn() bool { return false }
