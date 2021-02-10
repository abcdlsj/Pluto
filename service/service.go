package service

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"pluto/store"
	"strings"
)

type Store interface {
	Get(key string, level store.ConsistencyLevel) (string, error)
	Set(key, value string) error
	Delete(key string) error
	Join(nodeId string, httpAddr string, addr string) error
	LeaderAPIAddr() string
	SetMeta(key, value string) error
}

type Service struct {
	addr     string
	listener net.Listener
	store    Store
}

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if strings.HasPrefix(r.URL.Path, "/key") {
		s.handleKeyRequest(w, r)
	} else if r.URL.Path == "/join" {
		s.handleJoin(w, r)
	} else {
		w.WriteHeader(http.StatusNotFound)
	}
}

func (s *Service) Close() {
	s.listener.Close()
	return
}

// New returns an uninitialized HTTP service.
func New(addr string, store Store) *Service {
	return &Service{
		addr:  addr,
		store: store,
	}
}

func (s *Service) Start() error {
	server := http.Server{
		Handler: s,
	}
	listener, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	s.listener = listener
	http.Handle("/", s)
	go func() {
		err := server.Serve(s.listener)
		if err != nil {
			log.Fatalf("HTTP serve: %s", err)
		}
	}()

	return nil
}

func getLevel(req *http.Request) (store.ConsistencyLevel, error) {
	q := req.URL.Query()
	level := strings.TrimSpace(q.Get("level"))

	switch strings.ToLower(level) {
	case "default":
		return store.Default, nil
	case "stale":
		return store.Stale, nil
	case "consistent":
		return store.Consistent, nil
	default:
		return store.Default, nil
	}
}

func (s *Service) handleKeyRequest(w http.ResponseWriter, r *http.Request) {
	getKey := func() string {
		parts := strings.Split(r.URL.Path, "/")
		if len(parts) != 3 {
			return ""
		}
		return parts[2]
	}

	switch r.Method {
	case "GET":
		k := getKey()
		if k == "" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		level, err := getLevel(r)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		v, err := s.store.Get(k, level)
		if err != nil {
			if err == store.ErrNotLeader {
				leader := s.store.LeaderAPIAddr()
				if leader == "" {
					http.Error(w, err.Error(), http.StatusServiceUnavailable)
					return
				}
				redirect := s.FormRedirect(r, leader)
				//http.Redirect(w, r, redirect, http.StatusMovedPermanently)
				http.Redirect(w, r, redirect, http.StatusTemporaryRedirect)
				return
			}
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		b, err := json.Marshal(map[string]string{k: v})
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		io.WriteString(w, string(b))

	case "POST":
		// Read the value from the POST body.
		m := map[string]string{}
		if err := json.NewDecoder(r.Body).Decode(&m); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		for k, v := range m {
			if err := s.store.Set(k, v); err != nil {
				if err == store.ErrNotLeader {
					leader := s.store.LeaderAPIAddr()
					if leader == "" {
						http.Error(w, err.Error(), http.StatusServiceUnavailable)
						return
					}

					redirect := s.FormRedirect(r, leader)
					//http.Redirect(w, r, redirect, http.StatusMovedPermanently)
					http.Redirect(w, r, redirect, http.StatusTemporaryRedirect)
					return
				}

				w.WriteHeader(http.StatusInternalServerError)
				return
			}
		}
	case "DELETE":
		k := getKey()
		if k == "" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		if err := s.store.Delete(k); err != nil {
			if err == store.ErrNotLeader {
				leader := s.store.LeaderAPIAddr()
				if leader == "" {
					http.Error(w, err.Error(), http.StatusServiceUnavailable)
					return
				}
				redirect := s.FormRedirect(r, leader)
				//http.Redirect(w, r, redirect, http.StatusMovedPermanently)
				http.Redirect(w, r, redirect, http.StatusTemporaryRedirect)
				return
			}

			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		s.store.Delete(k)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
	return
}

func (s *Service) Addr() net.Addr {
	return s.listener.Addr()
}

func (s *Service) handleJoin(w http.ResponseWriter, r *http.Request) {
	m := map[string]string{}
	if err := json.NewDecoder(r.Body).Decode(&m); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if len(m) != 3 {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	httpAddr, ok := m["httpAddr"]
	if !ok {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	raftAddr, ok := m["raftAddr"]
	if !ok {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	nodeId, ok := m["id"]
	if !ok {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if err := s.store.Join(nodeId, httpAddr, raftAddr); err != nil {
		if err == store.ErrNotLeader {
			leader := s.store.LeaderAPIAddr()
			if leader == "" {
				http.Error(w, err.Error(), http.StatusServiceUnavailable)
				return
			}
			redirect := s.FormRedirect(r, leader)
			//http.Redirect(w, r, redirect, http.StatusMovedPermanently)
			http.Redirect(w, r, redirect, http.StatusTemporaryRedirect)
			return
		}
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}

// FormRedirect returns the value for the "Location" header for a 301 response.
func (s *Service) FormRedirect(r *http.Request, host string) string {
	protocol := "http"
	rq := r.URL.RawQuery
	if rq != "" {
		rq = fmt.Sprintf("?%s", rq)
	}
	return fmt.Sprintf("%s://%s%s%s", protocol, host, r.URL.Path, rq)
}
