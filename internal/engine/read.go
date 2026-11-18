package engine

import (
	"lamina/internal/datum"
	"lamina/internal/query"
)

func (d *DB) Query(q query.Request) (query.Result, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.usable(); err != nil {
		return query.Result{}, err
	}
	r, err := d.catalog.Relation(q.Relation)
	if err != nil {
		return query.Result{}, invalid(err)
	}
	at := d.sequence
	var tx *Transaction
	headView := false
	if q.Transaction != "" {
		if q.At != nil {
			return query.Result{}, fault("invalid", "at and transaction are mutually exclusive")
		}
		tx, err = d.tx(q.Transaction)
		if err != nil {
			return query.Result{}, err
		}
		switch q.View {
		case "", "pinned":
			// The edit keeps the read basis it started with and still reads
			// its own remaining private writes. Its already-published rows are
			// not replayed from the store, so another caller's intervening
			// values can never masquerade as the edit's original basis.
			at = tx.Snapshot
		case "head":
			// Explicitly opt into the current committed sequence with no
			// private overlay: the edit's earlier publications are ordinary
			// committed rows now, and other callers' new values are shown as
			// exactly what they are rather than as the edit's own reads.
			at = d.sequence
			headView = true
		default:
			return query.Result{}, fault("invalid", "view must be pinned or head")
		}
	} else if q.At != nil {
		if q.View != "" {
			return query.Result{}, fault("invalid", "view requires a transaction")
		}
		at = *q.At
	} else if q.View != "" {
		return query.Result{}, fault("invalid", "view requires a transaction")
	}
	if at < d.horizon || at > d.sequence {
		return query.Result{}, fault("snapshot", "snapshot unavailable; retained interval %d..%d", d.horizon, d.sequence)
	}
	if at < r.Created {
		return query.Result{}, fault("snapshot", "relation did not exist at snapshot")
	}
	if err = query.Bind(r, q); err != nil {
		return query.Result{}, invalid(err)
	}
	if tx != nil && !headView && len(tx.writes[q.Relation]) > 0 {
		q.ForceScan = true
	}
	plan, keys := query.Choose(d.catalog, d.indexes, q, at, len(d.store.Tables[q.Relation]))
	rows := map[string]datum.Row{}
	if plan.Access == "index" {
		for _, key := range keys {
			if row, exists := d.store.Read(q.Relation, key, at); exists {
				rows[key] = row
			}
		}
	} else {
		rows = d.store.Scan(q.Relation, at)
	}
	if tx != nil && !headView {
		overlay(rows, tx.writes, q.Relation)
	}
	result, err := query.Execute(r, q, rows, plan, keys)
	d.queries++
	if err != nil {
		return result, invalid(err)
	}
	return result, nil
}
func (d *DB) Get(id, table string, key datum.Value) (datum.Row, error) {
	q := query.Request{Relation: table, Transaction: id, Limit: 1}
	d.mu.RLock()
	r, err := d.catalog.Relation(table)
	d.mu.RUnlock()
	if err != nil {
		return nil, invalid(err)
	}
	col, _ := r.Column(r.Primary)
	if key.Null() || key.Kind != col.Type {
		return nil, fault("invalid", "invalid primary value")
	}
	q.Where = []query.Predicate{{Column: r.Primary, Op: "=", Value: key}}
	result, err := d.Query(q)
	if err != nil {
		return nil, err
	}
	if len(result.Rows) == 0 {
		return nil, fault("not_found", "tuple not found")
	}
	return result.Rows[0], nil
}
