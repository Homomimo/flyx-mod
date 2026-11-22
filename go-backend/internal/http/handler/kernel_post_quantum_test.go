package handler

import (
	"strings"
	"testing"

	"go-backend/internal/kernel"
	"go-backend/internal/store/repo"

	"go-backend/internal/store/model"
)

func TestKernelSupportsPostQuantumReality(t *testing.T) {
	for _, tc := range []struct {
		kernelType, installed string
		want                  bool
	}{
		{"xray", "", false},
		{"xray", "latest", true},
		{"xray", "99999999999999999999999.7.11", false},
		{"xray", "26.7.10", false},
		{"xray", "26.7.11", true},
		{"xray", "26.11.9", true},
		{"xray", "27.1.1", true},
		{"xray", "30.1.1", true},
		{"xray", "v26.11.9", true},
		{"xray", "Xray 26.11.9", true},
		{"xray", "Xray v26.7.11 (Xray, Penetrates Everything.)", true},
		{"xray", "Xray 26.7.10 (go1.26.8 linux/amd64)", false},
		{"xray", "Xray 26.7.11-beta (Xray, Penetrates Everything.)", false},
		{"xray", "unrecognized 26.7.11", false},
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

// Exercise the subscription path with the desired version stored independently
// of the observed version, just as it is after a kernel status report.
func TestXrayInstalledBannerInMihomoSubscription(t *testing.T) {
	r, err := repo.Open(t.TempDir() + "/xray-version.db")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.Close() })
	h := New(r, "test-secret")
	node := seedRelayTargetNode(t, r, "landing", "198.51.100.1")
	k := seedChainKernel(t, r, node, kernel.TypeXray)
	row := seedChainInbound(t, r, k, "Reality", kernel.Inbound{Protocol: kernel.ProtocolVLESS, Port: 443, Security: kernel.Security{Type: kernel.SecurityReality}}, 0)
	for _, tc := range []struct {
		desired, installed string
		want               bool
	}{
		{"26.7.11", "Xray 26.7.11 (Xray, Penetrates Everything.)", true},
		{"latest", "Xray 26.7.11 (Xray, Penetrates Everything.)", true},
		{"latest", "Xray 26.7.10 (Xray, Penetrates Everything.)", false},
		{"latest", "latest", true},
		{"latest", "", false},
	} {
		if err := r.UpdateNodeKernelFields(k.ID, map[string]interface{}{"version": tc.desired, "installed_version": tc.installed}); err != nil {
			t.Fatal(err)
		}
		ends, _ := h.inboundEndpoints([]model.NodeInbound{*row})
		if len(ends) != 1 {
			t.Fatalf("unexpected endpoint count %d", len(ends))
		}
		yaml, _, err := kernel.Subscription(kernel.FormatClash, ends, kernel.SubOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if got := strings.Contains(yaml, "support-x25519mlkem768: true"); got != tc.want {
			t.Fatalf("desired=%q installed=%q: got %v want %v\n%s", tc.desired, tc.installed, got, tc.want, yaml)
		}
	}
}
