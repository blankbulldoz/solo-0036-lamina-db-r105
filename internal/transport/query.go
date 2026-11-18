package transport

import (
	"lamina/internal/engine"
	"lamina/internal/query"
	"lamina/internal/syntax"
	"net/http"
)

func (s *Server) query(w http.ResponseWriter, r *http.Request) {
	var in query.Request
	if !decode(w, r, &in) {
		return
	}
	out, err := s.db.Query(in)
	if err != nil {
		respondError(w, err)
		return
	}
	write(w, 200, out)
}
func (s *Server) sql(w http.ResponseWriter, r *http.Request) {
	var in struct {
		SQL         string  `json:"sql"`
		Transaction string  `json:"transaction"`
		At          *uint64 `json:"at"`
		View        string  `json:"view"`
	}
	if !decode(w, r, &in) {
		return
	}
	q, err := syntax.Parse(in.SQL)
	if err != nil {
		respondError(w, &engine.Error{Code: "invalid", Message: err.Error()})
		return
	}
	q.Transaction = in.Transaction
	q.At = in.At
	q.View = in.View
	out, err := s.db.Query(q)
	if err != nil {
		respondError(w, err)
		return
	}
	write(w, 200, out)
}
