package transport

import (
	"lamina/internal/engine"
	"net/http"
)

func (s *Server) publish(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Selection []engine.Selection `json:"selection"`
		DryRun    bool               `json:"dry_run"`
	}
	if !decode(w, r, &in) {
		return
	}
	out, err := s.db.Publish(r.PathValue("tx"), in.Selection, in.DryRun)
	if err != nil {
		respondError(w, err)
		return
	}
	write(w, 200, out)
}

func (s *Server) refresh(w http.ResponseWriter, r *http.Request) {
	var in struct {
		DryRun bool `json:"dry_run"`
	}
	if !decode(w, r, &in) {
		return
	}
	out, err := s.db.Refresh(r.PathValue("tx"), in.DryRun)
	if err != nil {
		respondError(w, err)
		return
	}
	write(w, 200, out)
}

func (s *Server) txnStatus(w http.ResponseWriter, r *http.Request) {
	out, err := s.db.TxnStatus(r.PathValue("tx"))
	if err != nil {
		respondError(w, err)
		return
	}
	write(w, 200, out)
}
