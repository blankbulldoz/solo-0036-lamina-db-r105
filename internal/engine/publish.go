package engine

import (
	"lamina/internal/datum"
	"lamina/internal/journal"
	"lamina/internal/versions"
	"sort"
)

// Selection identifies one staged primary key inside an edit context. The
// server resolves the canonical key itself; clients never address journal
// internals.
type Selection struct {
	Relation string      `json:"relation"`
	Key      datum.Value `json:"key"`
}

type SelectedKey struct {
	Relation string `json:"relation"`
	Key      string `json:"key"`
}

type PublishReport struct {
	DryRun           bool              `json:"dry_run"`
	Admitted         bool              `json:"admitted"`
	Sequence         uint64            `json:"sequence,omitempty"`
	Writes           int               `json:"writes"`
	Selected         []SelectedKey     `json:"selected"`
	RemainingWrites  int               `json:"remaining_writes"`
	PartialPublishes uint64            `json:"partial_publishes"`
	Conflicts        []SelectedKey     `json:"conflicts,omitempty"`
	UniqueViolations []UniqueViolation `json:"unique_violations,omitempty"`
	// Unknown names staged keys that the edit context does not contain; their
	// presence makes the request invalid rather than silently ignored.
	Unknown []SelectedKey `json:"unknown,omitempty"`
}

type RefreshReport struct {
	DryRun               bool              `json:"dry_run"`
	FromSnapshot         uint64            `json:"from_snapshot"`
	ToSnapshot           uint64            `json:"to_snapshot"`
	Advanced             bool              `json:"advanced"`
	Conflicts            []SelectedKey     `json:"conflicts,omitempty"`
	UniqueViolations     []UniqueViolation `json:"unique_violations,omitempty"`
	NewlyVisible         []string          `json:"newly_visible_relations,omitempty"`
	OldViewStillReadable bool              `json:"old_view_still_readable"`
	RemainingWrites      int               `json:"remaining_writes"`
}

func (d *DB) stagedCount(tx *Transaction) int {
	count := 0
	for _, m := range tx.writes {
		count += len(m)
	}
	return count
}

func (d *DB) resolveSelection(tx *Transaction, selections []Selection) (map[string]map[string]staged, []SelectedKey, error) {
	chosen := map[string]map[string]staged{}
	var unknown []SelectedKey
	seen := map[string]bool{}
	for _, sel := range selections {
		relation, err := d.catalog.Relation(sel.Relation)
		if err != nil {
			return nil, nil, invalid(err)
		}
		col, _ := relation.Column(relation.Primary)
		if sel.Key.Null() || sel.Key.Kind != col.Type {
			return nil, nil, fault("invalid", "invalid primary value for relation %s", sel.Relation)
		}
		key := sel.Key.Key()
		token := sel.Relation + "\x00" + key
		if seen[token] {
			return nil, nil, fault("invalid", "duplicate selection for %s key %s", sel.Relation, key)
		}
		seen[token] = true
		m, ok := tx.writes[sel.Relation][key]
		if !ok {
			unknown = append(unknown, SelectedKey{Relation: sel.Relation, Key: key})
			continue
		}
		if chosen[sel.Relation] == nil {
			chosen[sel.Relation] = map[string]staged{}
		}
		chosen[sel.Relation][key] = m
	}
	return chosen, unknown, nil
}

func selectedList(chosen map[string]map[string]staged) []SelectedKey {
	out := []SelectedKey{}
	for table, mutations := range chosen {
		for key := range mutations {
			out = append(out, SelectedKey{Relation: table, Key: key})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Relation == out[j].Relation {
			return out[i].Key < out[j].Key
		}
		return out[i].Relation < out[j].Relation
	})
	return out
}

func conflictList(table string, keys []string) []SelectedKey {
	out := make([]SelectedKey, 0, len(keys))
	for _, key := range keys {
		out = append(out, SelectedKey{Relation: table, Key: key})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Relation == out[j].Relation {
			return out[i].Key < out[j].Key
		}
		return out[i].Relation < out[j].Relation
	})
	return out
}

// Publish atomically publishes the selected staged mutations as one complete
// commit while keeping the edit context open for the remaining mutations.
// Validation is identical to a full commit: first-committer-wins conflict
// checks and final-state unique checks run against committed head, never
// against the rest of the private edit. A rejected publish consumes no
// sequence and leaves every staged mutation in place.
func (d *DB) Publish(id string, selections []Selection, dryRun bool) (PublishReport, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.usable(); err != nil {
		return PublishReport{}, err
	}
	tx, err := d.tx(id)
	if err != nil {
		return PublishReport{}, err
	}
	if tx.ReadOnly {
		return PublishReport{}, fault("conflict", "read-only transaction")
	}
	if len(selections) == 0 {
		return PublishReport{}, fault("invalid", "publish requires at least one selected mutation")
	}
	chosen, unknown, err := d.resolveSelection(tx, selections)
	if err != nil {
		return PublishReport{}, err
	}
	report := PublishReport{DryRun: dryRun, Selected: selectedList(chosen), Unknown: unknown,
		RemainingWrites: d.stagedCount(tx), PartialPublishes: tx.PartialPublishes}
	if len(unknown) > 0 {
		return report, fault("invalid", "selection references mutations not staged in this edit")
	}

	// First-committer-wins: every selected key must be unchanged since the
	// edit's last accepted view. Refresh advances this base only after the
	// caller has inspected the reported conversion cost, so other callers'
	// commits are never silently adopted as the read basis.
	var conflicts []SelectedKey
	for table, mutations := range chosen {
		var keys []string
		for key := range mutations {
			if d.store.Latest(table, key) > tx.conflictBase {
				keys = append(keys, key)
			}
		}
		sort.Strings(keys)
		conflicts = append(conflicts, conflictList(table, keys)...)
	}
	var violations []UniqueViolation
	for table, mutations := range chosen {
		violations = append(violations, d.uniqueViolationsAt(d.sequence, table, mutations)...)
	}
	report.Conflicts = conflicts
	report.UniqueViolations = violations
	if len(conflicts) > 0 || len(violations) > 0 {
		if !dryRun {
			d.conflicts++
		}
		return report, &ReportError{
			Domain: fault("conflict", "partial publish rejected: resolve conflicts or unique violations first"),
			Report: report,
		}
	}

	writes := make([]versions.Mutation, 0)
	for _, mutations := range chosen {
		for _, m := range mutations {
			writes = append(writes, m.Mutation)
		}
	}
	sort.Slice(writes, func(i, j int) bool {
		if writes[i].Relation == writes[j].Relation {
			return writes[i].Key < writes[j].Key
		}
		return writes[i].Relation < writes[j].Relation
	})
	report.Writes = len(writes)
	if dryRun {
		report.Admitted = true
		return report, nil
	}
	if err = d.publish(journal.Record{Kind: "commit", Transaction: id, Edit: "partial", Writes: writes}); err != nil {
		return PublishReport{}, err
	}
	// Only after the durable publish succeeds do the selected rows leave the
	// private write set; the remaining edit keeps its pinned snapshot.
	for table, mutations := range chosen {
		for key := range mutations {
			delete(tx.writes[table], key)
		}
		if len(tx.writes[table]) == 0 {
			delete(tx.writes, table)
		}
	}
	tx.PartialPublishes++
	tx.PublishedAt = append(tx.PublishedAt, d.sequence)
	d.commits++
	d.partialPublishes++
	report.Admitted = true
	report.Sequence = d.sequence
	report.RemainingWrites = d.stagedCount(tx)
	report.PartialPublishes = tx.PartialPublishes
	return report, nil
}

// Refresh moves an edit context from its pinned snapshot to the current head.
// The cost of the conversion is computed first and returned for the caller to
// check: remaining writes that conflict with intervening commits, unique
// violations internal to the remaining edit at its own pinned read basis,
// relations that newly enter the catalog view, and whether the original view
// remains a readable retained version.
func (d *DB) Refresh(id string, dryRun bool) (RefreshReport, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.usable(); err != nil {
		return RefreshReport{}, err
	}
	tx, err := d.tx(id)
	if err != nil {
		return RefreshReport{}, err
	}
	report := RefreshReport{DryRun: dryRun, FromSnapshot: tx.Snapshot, ToSnapshot: d.sequence,
		Advanced: d.sequence > tx.Snapshot, OldViewStillReadable: d.horizon <= tx.OriginalSnapshot,
		RemainingWrites: d.stagedCount(tx)}
	var conflicts []SelectedKey
	var violations []UniqueViolation
	for table, mutations := range tx.writes {
		var keys []string
		for key := range mutations {
			if d.store.Latest(table, key) > tx.Snapshot {
				keys = append(keys, key)
			}
		}
		sort.Strings(keys)
		conflicts = append(conflicts, conflictList(table, keys)...)
		// Evaluate the remaining edit against its own pinned basis: this
		// surfaces internal problems such as an unapproved unique-value swap,
		// independent of what other callers committed in the meantime.
		violations = append(violations, d.uniqueViolationsAt(tx.Snapshot, table, mutations)...)
	}
	report.Conflicts = conflicts
	report.UniqueViolations = violations
	for _, relation := range d.catalog.List() {
		if relation.Created > tx.Snapshot && relation.Created <= d.sequence {
			report.NewlyVisible = append(report.NewlyVisible, relation.Name)
		}
	}
	sort.Strings(report.NewlyVisible)
	if dryRun {
		return report, nil
	}
	tx.Snapshot = d.sequence
	tx.conflictBase = d.sequence
	return report, nil
}

// TxnStatus describes an edit context including its partial-publish lineage.
func (d *DB) TxnStatus(id string) (Transaction, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if err := d.usable(); err != nil {
		return Transaction{}, err
	}
	tx, err := d.tx(id)
	if err != nil {
		return Transaction{}, err
	}
	out := *tx
	out.writes = nil
	out.Remaining = d.stagedCount(tx)
	return out, nil
}
