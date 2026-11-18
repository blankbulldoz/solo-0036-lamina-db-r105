package engine

import (
	"fmt"
	"lamina/internal/catalog"
	"lamina/internal/index"
	"lamina/internal/journal"
	"lamina/internal/snapshot"
	"lamina/internal/versions"
	"os"
	"path/filepath"
	"sync"
)

type DB struct {
	mu               sync.RWMutex
	dir              string
	catalog          *catalog.Catalog
	store            *versions.Store
	indexes          *index.Registry
	wal              *journal.WAL
	sequence         uint64
	horizon          uint64
	transactions     map[string]*Transaction
	closed           bool
	commits          uint64
	conflicts        uint64
	queries          uint64
	partialPublishes uint64
}

func Open(dir string) (*DB, error) {
	absolute, err := filepath.Abs(dir)
	if err != nil {
		return nil, err
	}
	image, err := snapshot.Read(filepath.Join(absolute, "checkpoint.lmi"))
	missing := os.IsNotExist(err)
	if err != nil && !missing {
		return nil, fmt.Errorf("checkpoint: %w", err)
	}
	if !missing {
		if _, err = os.Stat(filepath.Join(absolute, "journal.lmw")); err != nil {
			return nil, fmt.Errorf("checkpoint without journal; use validated restore")
		}
	}
	w, err := journal.Open(absolute, image.Sequence)
	if err != nil {
		return nil, err
	}
	d := &DB{dir: absolute, catalog: catalog.New(), store: versions.New(), wal: w, transactions: map[string]*Transaction{}}
	if !missing {
		d.catalog = image.Catalog
		d.store = image.Store
		d.sequence = image.Sequence
		d.horizon = image.Horizon
	}
	last := w.Base + uint64(len(w.Records))
	if d.sequence > last {
		w.Close()
		return nil, fmt.Errorf("checkpoint ahead of journal")
	}
	for _, r := range w.Records {
		if r.Sequence <= d.sequence {
			continue
		}
		if err = d.apply(r); err != nil {
			w.Close()
			return nil, fmt.Errorf("replay sequence %d: %w", r.Sequence, err)
		}
	}
	d.indexes = index.Build(d.catalog, d.store)
	for _, relation := range d.catalog.List() {
		if err = d.unique(relation.Name, nil); err != nil {
			w.Close()
			return nil, fmt.Errorf("recovered constraints: %w", err)
		}
	}
	return d, nil
}
func (d *DB) usable() error {
	if d.closed {
		return fault("unavailable", "database closed")
	}
	if d.wal.Poisoned {
		return fault("unavailable", "journal write outcome uncertain; restart required")
	}
	return nil
}
func (d *DB) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return nil
	}
	d.closed = true
	d.transactions = map[string]*Transaction{}
	return d.wal.Close()
}
func (d *DB) publish(r journal.Record) error {
	r.Sequence = d.sequence + 1
	r.Format = 1
	if err := d.wal.Append(r); err != nil {
		return fault("unavailable", "durable commit failed: %s", err)
	}
	if err := d.apply(r); err != nil {
		d.wal.Poisoned = true
		return fault("unavailable", "durable record cannot be applied: %s", err)
	}
	if len(r.Writes) > 0 {
		d.indexes.Apply(d.catalog, r.Writes)
	}
	return nil
}
func (d *DB) Relations() ([]catalog.Relation, []catalog.Index, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if err := d.usable(); err != nil {
		return nil, nil, err
	}
	return d.catalog.List(), d.catalog.IndexList(), nil
}
