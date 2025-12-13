package model

import (
	"time"

	"indexer/store"
)

type ASearchDoc struct {
	AID       string    `json:"a_id"`
	Status    string    `json:"a_status"`
	UpdatedAt time.Time `json:"updated_at"`

	B *BInline  `json:"b,omitempty"`
	C []CInline `json:"c,omitempty"`
}

type BInline struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type CInline struct {
	ID    string `json:"id"`
	Type  string `json:"type"`
	State string `json:"state"`
}

type BSearchDoc struct {
	BID       string    `json:"b_id"`
	Name      string    `json:"b_name"`
	ACount    int       `json:"a_count"`
	UpdatedAt time.Time `json:"updated_at"`
}

type CSearchDoc struct {
	CID       string    `json:"c_id"`
	Type      string    `json:"c_type"`
	State     string    `json:"c_state"`
	ACount    int       `json:"a_count"`
	UpdatedAt time.Time `json:"updated_at"`
}

func BuildADoc(a *store.AProj, b *store.BProj, cs []*store.CProj) ASearchDoc {
	doc := ASearchDoc{
		AID:       a.AID,
		Status:    a.Status,
		UpdatedAt: time.Now(),
	}
	if b != nil {
		doc.B = &BInline{ID: b.BID, Name: b.Name}
	}
	for _, c := range cs {
		if c == nil {
			continue
		}
		doc.C = append(doc.C, CInline{ID: c.CID, Type: c.Type, State: c.State})
	}
	return doc
}

func BuildBDoc(b *store.BProj, aCount int) BSearchDoc {
	return BSearchDoc{
		BID:       b.BID,
		Name:      b.Name,
		ACount:    aCount,
		UpdatedAt: time.Now(),
	}
}

func BuildCDoc(c *store.CProj, aCount int) CSearchDoc {
	return CSearchDoc{
		CID:       c.CID,
		Type:      c.Type,
		State:     c.State,
		ACount:    aCount,
		UpdatedAt: time.Now(),
	}
}
