package handler

import (
	"testing"

	"go-backend/internal/store/model"
)

func TestKernelSupportsPostQuantumReality(t *testing.T) {
	for _, tc := range []struct {
		kernelType, installed string
		want                  bool
	}{
		{"xray", "26.7.10", false},
		{"xray", "26.7.11", true},
		{"xray", "26.11.9", true},
		{"xray", "27.1.1", true},
		{"xray", "30.1.1", true},
		{"xray", "v26.11.9", false},
		{"xray", "Xray 26.11.9", false},
		{"xray", "26.11", false},
		{"xray", "26.11.9-beta", false},
		{"singbox", "1.12.0", false},
		{"sing-box", "1.13.0", false},
	} {
		got := kernelSupportsPostQuantumReality(&model.NodeKernel{KernelType: tc.kernelType, InstalledVersion: tc.installed})
		if got != tc.want {
			t.Errorf("%s %q: got %v want %v", tc.kernelType, tc.installed, got, tc.want)
		}
	}
	if kernelSupportsPostQuantumReality(nil) {
		t.Error("missing kernel")
	}
}
