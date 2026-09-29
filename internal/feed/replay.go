package feed

import (
	"strconv"
	"strings"

	"github.com/google/uuid"
)

// FormatTransitionPayload 把一次 feed 切换的观测阶段序列化为事件负载。
// 末位为新路由 revision；旧版负载没有该字段，解析端按缺字段容忍。
func FormatTransitionPayload(t Transition) string {
	return strings.Join([]string{
		t.Stage,
		t.OldSource,
		strconv.FormatBool(t.OldOpen),
		strconv.FormatFloat(t.OldFlow, 'f', -1, 64),
		t.NewSource,
		strconv.FormatBool(t.NewOpen),
		t.NewRevision,
	}, ":")
}

// ParseTransitionPayload 容忍式解析：旧版 6 段负载（无 revision）也能读。
func ParseTransitionPayload(payload string) (Transition, error) {
	parts := strings.Split(payload, ":")
	t := Transition{Stage: parts[0]}
	get := func(i int) string {
		if i < len(parts) {
			return parts[i]
		}
		return ""
	}
	t.OldSource = get(1)
	t.OldOpen = get(2) == "true"
	t.OldFlow, _ = strconv.ParseFloat(get(3), 64)
	t.NewSource = get(4)
	t.NewOpen = get(5) == "true"
	t.NewRevision = get(6)
	return t, nil
}

// Replay 按一条已持久的 feed.transition 事件重建路由状态，不生成新 uuid，
// 因此重放不会让 analyzer 绑定的 feed revision 漂移，也不重复触发 Observe。
func (c *Coordinator) Replay(t Transition) {
	old := &Route{Source: t.OldSource, Open: t.OldOpen, Flow: t.OldFlow}
	if t.Stage == "new-active" {
		c.Previous = old
		next := &Route{Source: t.NewSource, Open: t.NewOpen, Flow: 0}
		if t.NewOpen {
			next.Flow = 1
		}
		if t.NewRevision != "" {
			if id, err := uuid.Parse(t.NewRevision); err == nil {
				next.Revision = id
			}
		}
		c.Current = next
		return
	}
	// old-isolated：被切走的旧路由已关闭。
	c.Current = old
}
