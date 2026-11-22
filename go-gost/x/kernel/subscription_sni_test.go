package kernel

import (
	"fmt"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func TestSNIQueryDisabledAndValidation(t *testing.T) {
	now := time.Now().Truncate(time.Hour).UnixMilli()
	q := SNIQuery{7, now, now + 3600000}
	db := filepath.Join(t.TempDir(), "stats.db")
	v, err := querySNI(q, db, now)
	if err != nil || v.State != "disabled" {
		t.Fatal(v, err)
	}
	q.End++
	if _, err = querySNI(q, db, now); err == nil {
		t.Fatal("accepted unaligned interval")
	}
}
func TestSNIReadOnlyStore(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 CLI unavailable")
	}
	t0 := time.Now().Truncate(time.Hour).Unix()
	db := filepath.Join(t.TempDir(), "stats.db")
	sql := fmt.Sprintf(`create table health(sub,seen);insert into health values(7,%d);create table hits(sub,t,host,n);insert into hits values(7,%d,'docs.test',2),(7,%d,'docs.test',3),(8,%d,'other.test',9),(7,%d,'next.test',9);`, t0, t0, t0+60, t0, t0+3600)
	if b, e := exec.Command("sqlite3", db, sql).CombinedOutput(); e != nil {
		t.Fatal(string(b), e)
	}
	v, e := querySNI(SNIQuery{7, t0 * 1000, (t0 + 3600) * 1000}, db, t0*1000)
	if e != nil || v.State != "ready" || len(v.Items) != 1 || v.Items[0] != (SNIItem{"docs.test", 5}) {
		t.Fatal(v, e)
	}
}
