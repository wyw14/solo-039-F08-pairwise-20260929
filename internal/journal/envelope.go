package journal

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"

	"github.com/wyw14/solo-039/internal/model"
)

const envelopeVersion = 1

// 验链哨兵错误，供扫描时区分断链 / 序号问题。
var (
	errChainBroken = errors.New("event chain broken")
	errSeqGap      = errors.New("event seq gap")
)

// Envelope 是事件落盘的链式信封。
// Seq 从 1 开始严格连续；PrevDigest 指向上一条信封的 Digest（首条为空）；
// Digest 同时覆盖事件全部字段与 PrevDigest，使任一字段被改或任一行被
// 删除/重排/重写都会在验链时暴露。
type Envelope struct {
	Version    int         `json:"v"`
	Seq        uint64      `json:"seq"`
	PrevDigest string      `json:"prev"`
	Digest     string      `json:"digest"`
	Event      model.Event `json:"event"`
}

// hashFields 用 4 字节大端长度前缀分隔每个字段后求 sha256，
// 避免用 strings.Join 时 payload 内含分隔符可能构造出的碰撞。
func hashFields(fields ...string) string {
	h := sha256.New()
	var lenBuf [4]byte
	for _, field := range fields {
		binary.BigEndian.PutUint32(lenBuf[:], uint32(len(field)))
		h.Write(lenBuf[:])
		h.Write([]byte(field))
	}
	return hex.EncodeToString(h.Sum(nil))
}

// eventDigest 计算单条信封摘要。字段顺序固定，不得调整（属持久格式）。
func eventDigest(seq uint64, prev string, e model.Event) string {
	return hashFields(
		"v"+strconv.Itoa(envelopeVersion),
		strconv.FormatUint(seq, 10),
		e.ID.String(),
		e.Kind,
		e.Campaign.String(),
		e.Revision.String(),
		strconv.FormatUint(e.Epoch, 10),
		strconv.FormatInt(e.At, 10),
		e.Payload,
		prev,
	)
}

// NewEnvelope 构造一条已带摘要的链式信封。
func NewEnvelope(seq uint64, prev string, e model.Event) Envelope {
	return Envelope{
		Version:    envelopeVersion,
		Seq:        seq,
		PrevDigest: prev,
		Digest:     eventDigest(seq, prev, e),
		Event:      e,
	}
}

// Verify 校验信封：版本、序号连续性、genesis 规则与摘要。
// prev 为期望的上一条摘要，wantSeq 为期望序号。
func (env Envelope) Verify(prev string, wantSeq uint64) error {
	if env.Version != envelopeVersion {
		return fmt.Errorf("envelope version mismatch: got %d", env.Version)
	}
	if env.Seq != wantSeq {
		return fmt.Errorf("%w: got %d want %d", errSeqGap, env.Seq, wantSeq)
	}
	if wantSeq == 1 {
		if env.PrevDigest != "" {
			return errors.New("genesis envelope must carry empty prev digest")
		}
	} else if env.PrevDigest != prev {
		return fmt.Errorf("%w: prev digest mismatch", errChainBroken)
	}
	if env.Digest == "" {
		return errors.New("event digest is empty")
	}
	if env.Digest != eventDigest(env.Seq, env.PrevDigest, env.Event) {
		return errors.New("event digest mismatch")
	}
	return nil
}
