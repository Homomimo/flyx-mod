package handler

import (
	"context"
	"net/http"
	"sort"
	"sync"
	"time"

	"go-backend/internal/http/response"
)

type sniItem struct {
	Domain string `json:"domain"`
	Hits   int64  `json:"hits"`
}
type nodeSNI struct {
	State   string    `json:"state"`
	Items   []sniItem `json:"items"`
	Limited bool      `json:"limited"`
}
type sniView struct {
	Items       []sniItem `json:"items"`
	Ready       int       `json:"ready"`
	Stopped     int       `json:"stopped"`
	Disabled    int       `json:"disabled"`
	Unavailable int       `json:"unavailable"`
	Limited     bool      `json:"limited"`
}

// Use exactly the same calendar buckets as subscription/traffic, including fixed viewer offset.
func sniBounds(q subTrafficRequest, start int64, now time.Time) (int64, bool) {
	if q.TZOffset < -840 || q.TZOffset > 840 || (q.Range != "hour" && q.Range != "day" && q.Range != "month") {
		return 0, false
	}
	for _, t := range subTrafficBuckets(q.Range, now.In(time.FixedZone("", q.TZOffset*60))) {
		if t.UnixMilli() == start {
			switch q.Range {
			case "day":
				return t.AddDate(0, 0, 1).UnixMilli(), true
			case "month":
				return t.AddDate(0, 1, 0).UnixMilli(), true
			default:
				return t.Add(time.Hour).UnixMilli(), true
			}
		}
	}
	return 0, false
}
func (h *Handler) subscriptionSNI(w http.ResponseWriter, r *http.Request) {
	var req struct {
		subTrafficRequest
		Start int64 `json:"start"`
	}
	if r.Method != "POST" || decodeJSON(r.Body, &req) != nil {
		response.WriteJSON(w, response.ErrDefault("请求参数错误"))
		return
	}
	end, ok := sniBounds(req.subTrafficRequest, req.Start, time.Now())
	if !ok {
		response.WriteJSON(w, response.ErrDefault("无效的统计时段，请刷新页面"))
		return
	}
	sub, err := h.repo.GetSubscription(req.ID)
	if err != nil || sub == nil {
		response.WriteJSON(w, response.ErrDefault("订阅不存在"))
		return
	}
	view := sniView{Items: []sniItem{}}
	if !isShare(sub) {
		response.WriteJSON(w, response.OK(view))
		return
	}
	c, err := h.loadSubContext()
	if err != nil {
		response.WriteJSON(w, response.ErrDefault("读取节点失败"))
		return
	}
	want := map[int64]bool{}
	for _, id := range c.inboundIDs(sub) {
		want[id] = true
	}
	nodes := map[int64]bool{}
	for _, row := range c.rows {
		if want[row.ID] && row.SourceID == 0 && row.NodeID > 0 {
			nodes[row.NodeID] = true
		}
	}
	// Query current, locally managed nodes only. Never forward browsing metadata requests across federations.
	queue := make(chan int64, len(nodes))
	for id := range nodes {
		queue <- id
	}
	close(queue)
	results := make(chan nodeSNI, len(nodes))
	var wg sync.WaitGroup
	ctx, cancel := context.WithTimeout(r.Context(), 6*time.Second)
	defer cancel()
	for i := 0; i < min(6, len(nodes)); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for id := range queue {
				if ctx.Err() != nil {
					return
				}
				v := nodeSNI{}
				n, e := h.getNodeRecord(id)
				if e == nil && n != nil && n.IsRemote == 0 {
					res, e := h.wsServer.SendCommand(id, "SubscriptionSNI", map[string]interface{}{"id": req.ID, "start": req.Start, "end": end}, 3*time.Second)
					if e == nil && res.Success {
						if remarshal(res.Data, &v) != nil {
							v = nodeSNI{}
						}
					}
				}
				results <- v
			}
		}()
	}
	wg.Wait()
	close(results)
	view.Unavailable = len(nodes) - len(results) // Nodes not reached before the request deadline.
	counts := map[string]int64{}
	for v := range results {
		switch v.State {
		case "ready":
			view.Ready++
		case "stopped":
			view.Stopped++
		case "disabled":
			view.Disabled++
			continue
		default:
			view.Unavailable++
			continue
		}
		view.Limited = view.Limited || v.Limited
		for i, it := range v.Items {
			if i >= 100 {
				view.Limited = true
				break
			}
			if len(it.Domain) > 0 && len(it.Domain) <= 253 && it.Hits > 0 && it.Hits <= 1e12 {
				counts[it.Domain] += it.Hits
			}
		}
	}
	for d, n := range counts {
		view.Items = append(view.Items, sniItem{d, n})
	}
	sort.Slice(view.Items, func(i, j int) bool {
		a, b := view.Items[i], view.Items[j]
		if a.Hits == b.Hits {
			return a.Domain < b.Domain
		}
		return a.Hits > b.Hits
	})
	if len(view.Items) > 100 {
		view.Items = view.Items[:100]
		view.Limited = true
	}
	response.WriteJSON(w, response.OK(view))
}
