package kernel

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

const sniDB = "/var/lib/flyx-sni/stats.sqlite3"

type SNIQuery struct {
	ID    int64 `json:"id"`
	Start int64 `json:"start"`
	End   int64 `json:"end"`
}
type SNIItem struct {
	Domain string `json:"domain"`
	Hits   int64  `json:"hits"`
}
type SNIResult struct {
	State   string    `json:"state"`
	Items   []SNIItem `json:"items"`
	Limited bool      `json:"limited"`
}

// Query the optional observer's aggregate store; never open a capture or alter routing on panel request.
func QuerySNI(q SNIQuery) (SNIResult, error) {
	return querySNI(q, sniDB, time.Now().UnixMilli())
}

func querySNI(q SNIQuery, db string, now int64) (SNIResult, error) {
	out := SNIResult{State: "disabled"}
	if q.ID < 1 || q.ID > 65535 || q.Start <= 0 || q.Start%60000 != 0 || q.End%60000 != 0 || q.End <= q.Start || q.End-q.Start > 32*86400000 || q.Start > now || q.Start < now-400*86400000 {
		return out, errors.New("invalid SNI interval")
	}
	if _, err := os.Stat(db); os.IsNotExist(err) {
		return out, nil
	} else if err != nil {
		return out, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	// All substitutions are validated integers. No shell, no caller-selected paths.
	id := strconv.FormatInt(q.ID, 10)
	start := strconv.FormatInt(max(q.Start/1000, now/1000-30*86400), 10)
	end := strconv.FormatInt(q.End/1000, 10)
	sql := `SELECT COALESCE((SELECT seen FROM health WHERE sub=` + id + `),0); SELECT json_group_array(json_object('domain',host,'hits',total)) FROM (SELECT host,sum(n) total FROM hits WHERE sub=` + id + ` AND t>=` + start + ` AND t<` + end + ` GROUP BY host ORDER BY total DESC, host LIMIT 101);`
	buf, err := exec.CommandContext(ctx, "sqlite3", "-readonly", "-batch", "-noheader", db, sql).Output()
	if err != nil {
		return out, err
	}
	var seen int64
	parts := strings.SplitN(strings.TrimSpace(string(buf)), "\n", 2)
	if len(parts) != 2 {
		return out, errors.New("invalid SNI store response")
	}
	seen, err = strconv.ParseInt(strings.TrimSpace(parts[0]), 10, 64)
	if err != nil {
		return out, err
	}
	if err = json.Unmarshal([]byte(parts[1]), &out.Items); err != nil {
		return out, err
	}
	if seen > 0 {
		out.State = "ready"
		if now/1000-seen > 15 {
			out.State = "stopped"
		}
	}
	if len(out.Items) > 100 {
		out.Items = out.Items[:100]
		out.Limited = true
	}
	return out, nil
}
