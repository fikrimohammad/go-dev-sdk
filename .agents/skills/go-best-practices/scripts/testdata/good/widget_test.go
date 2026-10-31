package widget

import "testing"

func TestName(t *testing.T) {
	t.Run("empty id", func(t *testing.T) {
		if got, want := New("").Name(), ""; got != want {
			t.Errorf("New(%q).Name() = %q, want %q", "", got, want)
		}
	})
}
