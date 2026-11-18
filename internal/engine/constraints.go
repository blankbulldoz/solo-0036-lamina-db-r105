package engine

import (
	"lamina/internal/catalog"
	"lamina/internal/datum"
	"lamina/internal/index"
	"lamina/internal/journal"
	"sort"
)

func (d *DB) unique(table string, writes map[string]map[string]staged) error {
	rows := d.store.Scan(table, d.sequence)
	overlay(rows, writes, table)
	for _, i := range d.catalog.Indexes {
		if i.Relation == table && i.Unique {
			if err := checkUnique(rows, i); err != nil {
				return err
			}
		}
	}
	return nil
}

type UniqueViolation struct {
	Index string   `json:"index"`
	Value string   `json:"value"`
	Keys  []string `json:"keys"`
}

// uniqueViolationsAt builds the visible rows at base, overlays the supplied
// mutations, and reports every duplicate held under a unique index. Publish
// passes the current head; refresh passes the edit's pinned snapshot so the
// report describes what the remaining edit itself looks like at its own
// read basis.
func (d *DB) uniqueViolationsAt(base uint64, table string, writes map[string]staged) []UniqueViolation {
	rows := d.store.Scan(table, base)
	for key, m := range writes {
		if m.Mutation.Deleted {
			delete(rows, key)
		} else {
			rows[key] = datum.Clone(m.Mutation.Row)
		}
	}
	var out []UniqueViolation
	for _, definition := range d.catalog.Indexes {
		if definition.Relation != table || !definition.Unique {
			continue
		}
		holders := map[string][]string{}
		for key, row := range rows {
			v := row[definition.Column]
			if v.Null() {
				continue
			}
			token := v.Key()
			holders[token] = append(holders[token], key)
		}
		for value, keys := range holders {
			if len(keys) < 2 {
				continue
			}
			sort.Strings(keys)
			out = append(out, UniqueViolation{Index: definition.Name, Value: value, Keys: keys})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Index == out[j].Index {
			return out[i].Value < out[j].Value
		}
		return out[i].Index < out[j].Index
	})
	return out
}

func checkUnique(rows map[string]datum.Row, i catalog.Index) error {
	values := map[string]string{}
	for key, row := range rows {
		v := row[i.Column]
		if v.Null() {
			continue
		}
		token := v.Key()
		if prior, exists := values[token]; exists && prior != key {
			return fault("conflict", "unique constraint %s violated", i.Name)
		}
		values[token] = key
	}
	return nil
}
func (d *DB) CreateRelation(r catalog.Relation) (catalog.Relation, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.usable(); err != nil {
		return r, err
	}
	if err := r.Validate(); err != nil {
		return r, invalid(err)
	}
	if _, exists := d.catalog.Relations[r.Name]; exists {
		return r, fault("conflict", "relation exists")
	}
	r.Created = d.sequence + 1
	if err := d.publish(journal.Record{Kind: "relation", Relation: &r}); err != nil {
		return r, err
	}
	return r, nil
}
func (d *DB) CreateIndex(i catalog.Index) (catalog.Index, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.usable(); err != nil {
		return i, err
	}
	if err := d.catalog.CheckIndex(i); err != nil {
		return i, invalid(err)
	}
	if i.Unique {
		if err := checkUnique(d.store.Scan(i.Relation, d.sequence), i); err != nil {
			return i, err
		}
	}
	i.Created = d.sequence + 1
	if err := d.publish(journal.Record{Kind: "index", Index: &i}); err != nil {
		return i, err
	}
	d.indexes = index.Build(d.catalog, d.store)
	return i, nil
}
