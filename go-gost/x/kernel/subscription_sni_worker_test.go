package kernel

import (
	"context"
	"slices"
	"sync/atomic"
	"testing"
	"time"
)

func TestSNIMarkedSubscriptions(t *testing.T) {
	m := NewManager(t.TempDir())
	m.instances["one"] = &instance{req: ApplyRequest{Enabled: true, UserMarks: []int64{12, 7, 7, 0, 65536}}}
	m.instances["two"] = &instance{req: ApplyRequest{Enabled: false, UserMarks: []int64{9}}}
	if ids := m.sniIDs(); !slices.Equal(ids, []int64{7, 12}) {
		t.Fatal(ids)
	}
}
func TestSNIAutoLifecycle(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var desired atomic.Pointer[[]int64]
	set := func(ids ...int64) { desired.Store(&ids) }
	set(7)
	events := make(chan string, 20)
	fail := make(chan error, 1)
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		superviseSNI(ctx, time.Millisecond, 2*time.Millisecond, func() []int64 { return *desired.Load() }, func(ids []int64) (sniWorker, error) {
			label := "seven"
			if slices.Contains(ids, int64(12)) {
				label = "twelve"
			}
			events <- label
			return sniWorker{func() { events <- "stop" }, fail}, nil
		})
	}()
	expect := func(want string) {
		t.Helper()
		select {
		case e := <-events:
			if e != want {
				t.Fatalf("got %s want %s", e, want)
			}
		case <-time.After(time.Second):
			t.Fatal("timeout", want)
		}
	}
	expect("seven")
	set(7, 12)
	expect("stop")
	expect("twelve")
	fail <- nil
	expect("stop")
	expect("twelve")
	set()
	expect("stop")
	set(12)
	expect("twelve")
	cancel()
	expect("stop")
	<-finished
}
