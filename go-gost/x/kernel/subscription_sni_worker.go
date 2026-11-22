package kernel

import (
	"context"
	_ "embed"
	"encoding/json"
	"io"
	"log"
	"os"
	"os/exec"
	"runtime"
	"slices"
	"sync"
	"time"
)

//go:embed sniworker/collector.py
var collectorScript string

type managedSNI struct {
	mu     sync.Mutex
	cancel context.CancelFunc
	done   chan struct{}
}

func (s *managedSNI) stop() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cancel != nil {
		s.cancel()
		<-s.done
		s.cancel = nil
	}
}

func (m *Manager) sniIDs() []int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	var ids []int64
	for _, inst := range m.instances {
		if inst.req.Enabled {
			ids = append(ids, inst.req.UserMarks...)
		}
	}
	return sortedUsers(ids)
}

// Only the Agent lifecycle starts the worker, never a read from the panel.
func (m *Manager) StartSubscriptionSNI(parent context.Context) {
	if runtime.GOOS != "linux" {
		return
	}
	s := &m.sni
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cancel != nil {
		return
	}
	ctx, cancel := context.WithCancel(parent)
	s.cancel = cancel
	s.done = make(chan struct{})
	go func() { defer close(s.done); superviseSNI(ctx, 3*time.Second, time.Minute, m.sniIDs, startSNIWorker) }()
}

type sniWorker struct {
	close func()
	done  <-chan error
}

func startSNIWorker(ids []int64) (sniWorker, error) {
	cmd := exec.Command("python3", "-I", "-B", "-c", collectorScript, sniDirectory())
	cmd.Stderr = os.Stderr
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return sniWorker{}, err
	}
	if err = cmd.Start(); err != nil {
		stdin.Close()
		return sniWorker{}, err
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait(); close(done) }()
	if err = json.NewEncoder(stdin).Encode(ids); err != nil {
		stdin.Close()
		_ = cmd.Process.Kill()
		<-done
		return sniWorker{}, err
	}
	return sniWorker{func() { closeSNIProcess(stdin, cmd, done) }, done}, nil
}
func closeSNIProcess(pipe io.Closer, cmd *exec.Cmd, done <-chan error) {
	_ = pipe.Close()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		_ = cmd.Process.Kill()
		<-done
	}
}

func superviseSNI(ctx context.Context, interval, backoff time.Duration, ids func() []int64, start func([]int64) (sniWorker, error)) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	var current []int64
	var w sniWorker
	var retry time.Time
	stop := func() {
		if w.close != nil {
			w.close()
			w = sniWorker{}
		}
	}
	defer stop()
	for ctx.Err() == nil {
		next := ids()
		if !slices.Equal(current, next) {
			stop()
			current = next
			retry = time.Time{}
		}
		if len(current) > 0 && w.close == nil && !time.Now().Before(retry) {
			var err error
			w, err = start(current)
			if err != nil {
				log.Printf("SNI collector could not start: %v", err)
				retry = time.Now().Add(backoff)
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case err := <-w.done:
			log.Printf("SNI collector exited; will retry: %v", err)
			stop()
			retry = time.Now().Add(backoff)
		}
	}
}
