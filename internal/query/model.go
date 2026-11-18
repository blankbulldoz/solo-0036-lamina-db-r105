package query

import "lamina/internal/datum"

type Predicate struct {
	Column string      `json:"column"`
	Op     string      `json:"op"`
	Value  datum.Value `json:"value"`
}
type Ordering struct {
	Column string `json:"column"`
	Desc   bool   `json:"desc"`
}
type Aggregate struct {
	Function string `json:"function"`
	Column   string `json:"column,omitempty"`
}
type Request struct {
	Relation    string      `json:"relation"`
	Columns     []string    `json:"columns,omitempty"`
	Where       []Predicate `json:"where,omitempty"`
	Order       []Ordering  `json:"order,omitempty"`
	Limit       int         `json:"limit,omitempty"`
	At          *uint64     `json:"at,omitempty"`
	Transaction string      `json:"transaction,omitempty"`
	// View selects the read basis of a transactional query: "pinned" (the
	// default) freezes the edit's original snapshot and overlays its remaining
	// private writes, while "head" reads the current committed sequence with
	// no private overlay.
	View      string     `json:"view,omitempty"`
	Aggregate *Aggregate `json:"aggregate,omitempty"`
	ForceScan bool       `json:"force_scan,omitempty"`
}
type Plan struct {
	Access     string `json:"access"`
	Index      string `json:"index,omitempty"`
	Candidates int    `json:"candidates"`
	Snapshot   uint64 `json:"snapshot"`
	Residuals  int    `json:"residuals"`
}
type Result struct {
	Rows      []datum.Row `json:"rows"`
	Plan      Plan        `json:"plan"`
	Scanned   int         `json:"scanned"`
	Matched   int         `json:"matched"`
	Truncated bool        `json:"truncated"`
}
