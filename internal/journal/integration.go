package journal

import (
	"errors"
	"github.com/wyw14/solo-039/internal/model"
)

func PersistProof(store *Store, proof model.Proof, campaign model.Campaign) error {
	if proof.Digest == "" {
		proof.Digest = model.ProofDigest(proof)
	}
	event := model.NewEvent("proof.durable", campaign, proof.Digest)
	if err := store.Append(event); err != nil {
		return err
	}
	return nil
}

func RequireProof(store *Store, revision string) error {
	event, ok := store.Last("proof.durable")
	if !ok || event.Revision.String() != revision {
		return errors.New("proof is not durable")
	}
	return nil
}

// RequireProofConfirmed 在 RequireProof 之上，进一步要求该 proof.durable
// 事件位于检查点确认水位之内（即不仅已落盘，而且动作已确认）。
func RequireProofConfirmed(store *Store, revision string, confirmedSeq uint64) error {
	if err := RequireProof(store, revision); err != nil {
		return err
	}
	event, _ := store.Last("proof.durable")
	seq, ok := store.SeqOf(event.ID.String())
	if !ok || seq > confirmedSeq {
		return errors.New("proof is durable but not yet confirmed")
	}
	return nil
}
