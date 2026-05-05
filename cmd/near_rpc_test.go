package cmd

import "testing"

func TestRoundedStopAt(t *testing.T) {
	t.Parallel()

	got := roundedStopAt(196840534, 1000)
	if got != 196840000 {
		t.Fatalf("roundedStopAt() = %d, want %d", got, int64(196840000))
	}
}
