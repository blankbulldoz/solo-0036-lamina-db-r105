package journal

import (
	"lamina/internal/catalog"
	"lamina/internal/versions"
)

type Record struct {
	Format      int                 `json:"format"`
	Sequence    uint64              `json:"sequence"`
	Kind        string              `json:"kind"`
	Transaction string              `json:"transaction,omitempty"`
	Edit        string              `json:"edit,omitempty"`
	Relation    *catalog.Relation   `json:"relation,omitempty"`
	Index       *catalog.Index      `json:"index,omitempty"`
	Writes      []versions.Mutation `json:"writes,omitempty"`
	Horizon     uint64              `json:"horizon,omitempty"`
}
