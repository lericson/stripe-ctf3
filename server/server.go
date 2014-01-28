package server

import (
	"io"
	"bytes"
	"math/rand"
	"github.com/gorilla/mux"
	"io/ioutil"
	"net/http"
	"stripe-ctf.com/sqlcluster/log"
	"stripe-ctf.com/sqlcluster/sql"
	"stripe-ctf.com/sqlcluster/transport"
	"stripe-ctf.com/sqlcluster/util"
	"time"
)

type Term struct {
	number int
	voted  bool
}

type Server struct {
	name       string
	path       string
	listen     string
	router     *mux.Router
	httpServer *http.Server
	sql        *sql.SQL
	client     *transport.Client
	cluster    *Cluster
	term       Term
	seqNum     int
	disabled   bool
}

type Join struct {
	Self ServerAddress `json:"self"`
}

type JoinResponse struct {
	Self    ServerAddress   `json:"self"`
	Members []ServerAddress `json:"members"`
}

type AppendEntry struct {
	Self   ServerAddress `json:"self"`
	SeqNum int           `json:"seq"`
	Query  []byte        `json:"query"`
}

type AppendEntryResponse struct {
	Self ServerAddress `json:"self"`
}

// Creates a new server.
func New(path, listen string) (*Server, error) {
	cs, err := transport.Encode(listen)
	if err != nil {
		return nil, err
	}

	log.Printf("Server starting on %s backed by %s", listen, path)

	sql, err := sql.NewSQL(":memory:")
	if err != nil {
		return nil, err
	}

	s := &Server{
		path:    path,
		listen:  listen,
		sql:     sql,
		router:  mux.NewRouter(),
		client:  transport.NewClient(),
		cluster: NewCluster(path, cs),
	}

	return s, nil
}

// Starts the server.
func (s *Server) ListenAndServe(leader string) error {
	var err error
	// Initialize and start HTTP server.
	s.httpServer = &http.Server{
		Handler: s.router,
	}

	if leader == "" {
		s.cluster.Init()
	} else {
		s.Join(leader)
	}

	go func() {
		return // <-- NOTE!!
		for {
			n := s.seqNum

			time.Sleep(200 * time.Millisecond)

			if (s.cluster.State() == "leader") {
				s.emitAppendEntryRequest()
				continue
			}

			time.Sleep(50 * time.Millisecond)

			if (s.seqNum > n) {
				n = s.seqNum
				continue
			}

			s.cluster.LeaderDead()

			s.newTerm()
			rand.Seed(time.Now().UnixNano())

			time.Sleep(time.Duration((1.0 + rand.Float32()) * 150.0) * time.Millisecond)

			if (!s.term.voted) {
				log.Printf("No vote requests, candidating")
				s.candidate()
			}
		}
	}()

	s.router.HandleFunc("/join", s.joinHandler).Methods("POST")
	s.router.HandleFunc("/healthcheck", s.healthcheckHandler).Methods("GET")
	s.router.HandleFunc("/sql", s.sqlHandler).Methods("POST")
	s.router.HandleFunc("/sql/two-phase/begin", s.twoPhaseBeginHandler).Methods("POST")
	s.router.HandleFunc("/sql/two-phase/commit", s.twoPhaseCommitHandler).Methods("POST")
	s.router.HandleFunc("/entries", s.appendEntryHandler).Methods("POST")
	s.router.HandleFunc("/votes", s.voteRequestHandler).Methods("POST")
	s.router.HandleFunc("/ping", func(w http.ResponseWriter, req *http.Request) {
		w.Write([]byte("OK!"))
	}).Methods("GET")

	// Start Unix transport
	l, err := transport.Listen(s.listen)
	if err != nil {
		log.Fatal(err)
	}
	return s.httpServer.Serve(l)
}

// Client operations

func (s *Server) healthcheckLeader() bool {
	_, err := s.client.SafeGet(s.cluster.leader.ConnectionString, "/healthcheck")

	if err != nil {
		log.Printf("Leader healthcheck failed: %s", err)
		return false
	} else {
		return true
	}
}

// Join an existing cluster
func (s *Server) Join(leader string) error {
	join := &Join{Self: s.cluster.self}
	b := util.JSONEncode(join)

	cs, err := transport.Encode(leader)
	if err != nil {
		return err
	}

	for {
		body, err := s.client.SafePost(cs, "/join", b)
		if err != nil {
			log.Printf("Unable to join cluster: %s", err)
			time.Sleep(1 * time.Second)
			continue
		}

		resp := &JoinResponse{}
		if err = util.JSONDecode(body, &resp); err != nil {
			return err
		}

		s.cluster.Join(resp.Self, resp.Members)
		return nil
	}
}

func (s *Server) newTerm() {
	s.term = Term{
		number: s.term.number + 1,
		voted:  false,
	}
	log.Printf("Begun term %d", s.term.number)
}

func (s *Server) emitBytes(path string, body *bytes.Buffer) error {
	for _, member := range s.cluster.members {
		if member.Name == s.cluster.self.Name {
			continue
		}
		_, err := s.client.SafePost(member.ConnectionString, path, body)
		if err != nil {
			log.Printf("Ignored error in emit to %v: %s", member, err)
		}
	}
	return nil
}

func (s *Server) emitAppendEntryRequest() {
	s.seqNum += 1
	ae := &AppendEntry{
		Self: s.cluster.self,
		SeqNum: s.seqNum,
	}

	s.emitBytes("/entries/append", util.JSONEncode(ae))
}

/**
 * Emit vote requests to all members, return
 * true if candidacy resulted in leadership.
 */
func (s *Server) candidate() bool {
	// Step 1: vote for self
	s.seqNum += 1
	s.term.voted = true
	vr := &AppendEntry{
		Self: s.cluster.self,
		SeqNum: s.seqNum,
	}
	body := util.JSONEncode(vr)
	for _, member := range s.cluster.members {
		if member.Name == s.cluster.self.Name {
			continue
		}
		_, err := s.client.SafePost(member.ConnectionString, "/votes/request", body)
		if err != nil {
			log.Printf("Ignored error in emit to %v: %s", member, err)
		}
	}
	// Read response etc.
	return true
}

// Server handlers
func (s *Server) joinHandler(w http.ResponseWriter, req *http.Request) {
	j := &Join{}
	if err := util.JSONDecode(req.Body, j); err != nil {
		log.Printf("Invalid join request: %s", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	log.Printf("Handling join request: %#v", j)

	// Add node to the cluster
	if err := s.cluster.AddMember(j.Self); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}

	// Respond with the current cluster description
	resp := &JoinResponse{
		s.cluster.self,
		s.cluster.members,
	}
	b := util.JSONEncode(resp)
	w.Write(b.Bytes())
}

func (s *Server) twoPhaseBeginHandler(w http.ResponseWriter, req *http.Request) {
	query, err := ioutil.ReadAll(req.Body)
	if err != nil {
		log.Printf("Couldn't read body: %s", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
	}

	if s.cluster.State() != "leader" {
		log.Printf("Only leader can handle the two-phase response cycle")
		http.Error(w, "What the fuck, man?", http.StatusBadGateway)
		return
	}

	s.sql.Begin()

	resp, err := s.sql.Execute(string(query))
	if err != nil {
		log.Printf("\x1b[31mError\x1b[0m executing two-phase query: %s", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
	}

	w.Write(resp)
}

func (s *Server) twoPhaseCommitHandler(w http.ResponseWriter, req *http.Request) {
	s.sql.Commit()
	w.Write([]byte("OK"))
}

func (s *Server) sqlHandler(w http.ResponseWriter, req *http.Request) {
	query, err := ioutil.ReadAll(req.Body)
	if err != nil {
		log.Printf("Couldn't read body: %s", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
	}

	state := s.cluster.State()

	if s.disabled {
		log.Printf("Dropping request to disabled node")
		http.Error(w, "Disabled node", http.StatusBadGateway)
		return
	}

	if state != "leader" {
		if false && bytes.HasPrefix(query, []byte("CREATE TABLE ")) {
			log.Debugf("[%s] Ignoring table creation SQL", state)
			query = []byte("")
		}

		log.Debugf("[%s] \x1b[;33mForwarding query\x1b[0m: %#v", state, string(query))

		s.disabled = false

		/*
		_, err := s.client.SafeGet(s.cluster.leader.ConnectionString, "/ping")
		if err != nil {
			log.Printf("\x1b[31mError\x1b[0m during forward ping: %s", err)
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		*/

		resp, err := s.client.Post(s.cluster.leader.ConnectionString,
			                       "/sql/two-phase/begin",
			                       bytes.NewBuffer(query))

		// TODO Retry logic

		if err != nil {
			log.Printf("\x1b[31mError\x1b[0m beginning forwarded transaction: %s", err)
			http.Error(w, err.Error(), http.StatusBadGateway)
			//s.disabled = true
			//time.Sleep(10 * time.Millisecond)
			return
		}

		_, err = io.Copy(w, resp.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			log.Printf("\x1b[31mError\x1b[0m during forward of response: %s", err)
			//s.disabled = true
			//time.Sleep(10 * time.Millisecond)
		}

		commitResp, err := s.client.Post(s.cluster.leader.ConnectionString,
			"/sql/two-phase/commit",
			new(bytes.Buffer))
		commitBody, _ := ioutil.ReadAll(commitResp.Body)
		if string(commitBody) == "OK" {
			log.Printf("\x1b[32mCommitted\x1b[0m forwarded transaction")
			return
		}

		if err != nil {
			log.Printf("\x1b[31mError\x1b[0m committing forwarded transaction: %s", err)
			http.Error(w, err.Error(), http.StatusBadGateway)
			//s.disabled = true
			//time.Sleep(10 * time.Millisecond)
			return
		}

		log.Debugf("[%s] \x1b[;33mForwarded query\x1b[0m: %#v", state, string(query))

		return
	}

	// Add query to log
	// Send query to followers

	log.Debugf("[%s] \x1b[32mExecuting\x1b[0m query: %#v", state, string(query))
	s.sql.Begin()
	defer s.sql.Commit()
	resp, err := s.sql.Execute(string(query))
	if err != nil {
		log.Printf("\x1b[31mError\x1b[0m executing query: %s", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
	}

/*
	r := &Replicate{
		Self:  s.cluster.self,
		Query: query,
	}
	for _, member := range s.cluster.members {
		b := util.JSONEncode(r)
		_, err := s.client.SafePost(member.ConnectionString, "/replicate", b)
		if err != nil {
			log.Printf("Couldn't replicate query to %v: %s", member, err)
		}
	}
*/
	w.Write(resp)
}

func (s *Server) appendEntryHandler(w http.ResponseWriter, req *http.Request) {
	ae := &AppendEntry{}
	if err := util.JSONDecode(req.Body, ae); err != nil {
		log.Printf("Invalid append entry request: %s", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	log.Printf("Handling append entry request from %v", ae.Self)

	if ae.Query != nil {
		_, err := s.sql.Execute(string(ae.Query))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
		}
	}

	resp := &AppendEntryResponse{
		s.cluster.self,
	}
	b := util.JSONEncode(resp)
	w.Write(b.Bytes())
}

func (s *Server) confirmEntryHandler(w http.ResponseWriter, req *http.Request) {
	log.Printf("Confirmation not implemented")
	http.Error(w, "Not implemented", http.StatusInternalServerError)
}

func (s *Server) healthcheckHandler(w http.ResponseWriter, req *http.Request) {
	w.WriteHeader(http.StatusOK)
}

func (s *Server) voteRequestHandler(w http.ResponseWriter, req *http.Request) {
	// Do vote if we can and assign term and stuff
}

func (s *Server) voteCastHandler(w http.ResponseWriter, req *http.Request) {
	// A vote is cast
}


