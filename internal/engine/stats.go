package engine

import "sort"

type Stats struct {
	Sequence         uint64         `json:"sequence"`
	Horizon          uint64         `json:"horizon"`
	Active           []Transaction  `json:"active"`
	Keys             int            `json:"keys"`
	Versions         int            `json:"versions"`
	IndexEntries     map[string]int `json:"index_entries"`
	JournalRecords   int            `json:"journal_records"`
	HistoryBase      uint64         `json:"history_base"`
	RecoveredTail    int64          `json:"recovered_tail_bytes"`
	Commits          uint64         `json:"commits_since_open"`
	PartialPublishes uint64         `json:"partial_publishes_since_open"`
	Conflicts        uint64         `json:"conflicts_since_open"`
	Queries          uint64         `json:"queries_since_open"`
}

func (d *DB) Stats() (Stats, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if err := d.usable(); err != nil {
		return Stats{}, err
	}
	keys, versions := d.store.Count()
	s := Stats{Sequence: d.sequence, Horizon: d.horizon, Keys: keys, Versions: versions, Active: []Transaction{}, IndexEntries: map[string]int{}, JournalRecords: len(d.wal.Records), HistoryBase: d.wal.Base, RecoveredTail: d.wal.RecoveredTail, Commits: d.commits, PartialPublishes: d.partialPublishes, Conflicts: d.conflicts, Queries: d.queries}
	for _, tx := range d.transactions {
		copy := *tx
		copy.writes = nil
		copy.Remaining = d.stagedCount(tx)
		s.Active = append(s.Active, copy)
	}
	sort.Slice(s.Active, func(i, j int) bool { return s.Active[i].ID < s.Active[j].ID })
	for name, t := range d.indexes.Trees {
		s.IndexEntries[name] = t.Size()
	}
	return s, nil
}
