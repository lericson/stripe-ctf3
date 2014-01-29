package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"stripe-ctf.com/sqlcluster/sql"
	"stripe-ctf.com/sqlcluster/log"
	"stripe-ctf.com/sqlcluster/transport"
	"stripe-ctf.com/sqlcluster/command"
	"github.com/coreos/raft"
	"github.com/gorilla/mux"
	"io/ioutil"
	"math/rand"
	"net/http"
	"sync"
	"time"
)

// The raftd server is a combination of the Raft server and an HTTP
// server which acts as the transport.
type Server struct {
	name       string
	path       string
	listen     string
	router     *mux.Router
	raftServer raft.Server
	httpServer *http.Server
	sql        *sql.SQL
	client     *transport.Client
	mutex      sync.RWMutex
	leaderCh   chan string
}

// Creates a new server.
func New(path string, listen string) (*Server, error) {
	log.Printf("Server starting on %s backed by %s", listen, path)

	sqlDb, err := sql.New(":memory:")
	if err != nil {
		return nil, err
	}

	s := &Server{
		path:     path,
		listen:   listen,
		sql:      sqlDb,
		router:   mux.NewRouter(),
		client:   transport.NewClient(),
		leaderCh: make(chan string),
	}

	s.name = listen

	return s, nil
}

// Returns the connection string.
func (s *Server) connectionString() string {
	cs, _ := transport.Encode(s.listen)
	return cs
}

// Starts the server.
func (s *Server) ListenAndServe(leader string) error {
	var err error

	//raft.SetLogLevel(raft.Trace)

	log.Printf("Initializing Raft Server: %s", s.path)

	// Initialize and start Raft server.
	transporter := raft.NewHTTPTransporter("/raft")
	transporter.Transport.Dial = transport.UnixDialer
	s.raftServer, err = raft.NewServer(s.name, s.path, transporter, nil, s.sql, "")
	if err != nil {
		log.Fatal(err)
	}
	transporter.Install(s.raftServer, s)
	s.raftServer.SetElectionTimeout(1 * time.Second)
	s.raftServer.Start()

	s.raftServer.AddEventListener(raft.LeaderChangeEventType, func(e raft.Event) {
		go func() { s.leaderCh <-e.Value().(string) }()
	})

	if leader != "" {
		// Join to leader if specified.

		log.Println("Attempting to join leader:", leader)

		if !s.raftServer.IsLogEmpty() {
			log.Fatal("Cannot join with an existing log")
		}
		if err := s.Join(leader); err != nil {
			log.Fatal(err)
		}

	} else if s.raftServer.IsLogEmpty() {
		// Initialize the server by joining itself.

		log.Println("Initializing new cluster")

		_, err := s.raftServer.Do(&raft.DefaultJoinCommand{
			Name:             s.raftServer.Name(),
			ConnectionString: s.connectionString(),
		})
		if err != nil {
			log.Fatal(err)
		}

	} else {
		log.Println("Recovered from log")
	}

	log.Println("Initializing HTTP server")

	// Initialize and start HTTP server.
	s.httpServer = &http.Server{
		Handler: s.router,
	}

	//s.router.HandleFunc("/db/{key}", s.readHandler).Methods("GET")
	//s.router.HandleFunc("/db/{key}", s.writeHandler).Methods("POST")
	s.router.HandleFunc("/join", s.joinHandler).Methods("POST")
	s.router.HandleFunc("/sql", s.sqlHandler).Methods("POST")

	log.Println("Listening at:", s.connectionString())

	// Start Unix transport
	l, err := transport.Listen(s.listen)
	if err != nil {
		log.Fatal(err)
	}
	return s.httpServer.Serve(l)

	//return s.httpServer.ListenAndServe()
}

// This is a hack around Gorilla mux not providing the correct net/http
// HandleFunc() interface.
func (s *Server) HandleFunc(pattern string, handler func(http.ResponseWriter, *http.Request)) {
	s.router.HandleFunc(pattern, handler)
}

// Joins to the leader of an existing cluster.
func (s *Server) Join(leader string) error {
	command := &raft.DefaultJoinCommand{
		Name:             s.raftServer.Name(),
		ConnectionString: s.connectionString(),
	}

	var b bytes.Buffer
	json.NewEncoder(&b).Encode(command)
	//resp, err := http.Post(fmt.Sprintf("http://%s/join", leader), "application/json", &b)
	cs, _ := transport.Encode(leader)
	_, err := s.client.SafePost(cs, "/join", &b)
	return err
}

func (s *Server) joinHandler(w http.ResponseWriter, req *http.Request) {
	command := &raft.DefaultJoinCommand{}

	if err := json.NewDecoder(req.Body).Decode(&command); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if _, err := s.raftServer.Do(command); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (s *Server) sqlHandler(w http.ResponseWriter, req *http.Request) {
	var query, txId string
	var resp []byte
	var err error

	// Read the value from the POST body.
	b, err := ioutil.ReadAll(req.Body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	query = string(b)
	rand.Seed(time.Now().UnixNano())

	switch req.URL.RawQuery {
	case "":
		txId = fmt.Sprintf("%08x", time.Now().UnixNano() & 0xffffffff)
		break
	default:
		txId = req.URL.RawQuery
		break
	}

	if cachedResp := s.sql.QueryCache[txId]; cachedResp != nil {
		log.Printf("%s returning cached query", txId)
		w.Write(cachedResp)
		return
	}

	for state := s.raftServer.State(); !((state == raft.Follower) || (state == raft.Leader)); {
		log.Printf("State %s, waiting for network to stabilize", state)
		if len(s.raftServer.Peers()) != 4 {
			log.Printf("Peer count is weird!")
			for _, peer := range s.raftServer.Peers() {
				log.Printf("- Peer %s at %s", peer.Name, peer.ConnectionString)
			}
		}
		time.Sleep(300*time.Millisecond)
	}

	state := s.raftServer.State()

	if state == raft.Follower {
		ch := make(chan sql.Result)
		//s.sql.ResultChannels[txId] = ch
		//defer func() { delete(s.sql.ResultChannels, txId); }()
		log.Printf("%s Expect follower proxy SequenceNumber", txId)
		t := rand.Int()&0xffff
		errCh := make(chan int)
postLoop:
		for i := 1; ; i++ {
			log.Debugf("Beginning proxy attempt %04x#%d", t, i)
			go func() {
				var connString string
				log.Debugf("Proxy request goroutine %04x#%d started", t, i)
				leaderName := s.raftServer.Leader()
				for connString == "" {
					switch {
					case leaderName == s.raftServer.Name():
						connString = s.connectionString()
						break
					case leaderName == "":
						break
					default:
						peers := s.raftServer.Peers()
						peer := peers[leaderName]
						if peer == nil {
							log.Printf("Leader %v does not exist in peers map", leaderName)
							errCh <-1
							return
						}
						connString = peer.ConnectionString
						break
					}
					if connString == "" {
						log.Printf("Waiting for raft to elect a new leader %04x#%d", t, i)
						leaderName = <-s.leaderCh
					}
				}

				var body []byte
				log.Debugf("Pre-proxy goroutine %04x#%d", t, i)
				resp, err := s.client.SafePost(connString, "/sql?" + txId, bytes.NewBuffer(b))
				log.Debugf("Post proxy request for attempt %04x#%d", t, i)
				if err != nil {
					// When an error occurs we signal that error later so that
					// if it got through, it'll maybe get back to us.
					log.Printf("Failed sending proxy request: %s", err)
					errCh <-1
					return
				}
				body, err = ioutil.ReadAll(resp)
				if err != nil {
					log.Printf("Failed reading proxy response: %s", err)
					errCh <-1
					return
				}

				log.Printf("marikan Successfully posted %s: %s", txId, string(body))
				ch <- sql.Result{
					Output: body,
					Error:  nil,
				}
			}()
			//log.Printf("marikan Waiting for results on %s", txId)
			select {
			case result := <-ch:
				resp = result.Output
				err = result.Error
				//log.Printf("marikan Before break proxy response: %s", string(resp))
				break postLoop
			case <-errCh:
				log.Printf("%s Error in proxy attempt, retrying", txId)
				time.Sleep(250 * time.Millisecond)
				continue postLoop
			/*case <-time.After(2*time.Second):
				log.Printf("No SequenceNumber in attempt %04x#%d, retrying", t, i)
				continue postLoop*/
			}
		}
		//log.Printf("Follower proxy response: %s", string(resp))
		w.Write(resp)
		return
	} else {
		log.Debugf("Rafting SQL query")
		// Execute the command against the Raft server.
		resp_, err_ := s.raftServer.Do(command.NewSQLCommand(txId, query))
		err = err_
		if err == nil {
			resp = resp_.([]byte)
		} else {
			log.Printf("Rafting error: %s", err)
		}
		log.Debugf("Rafting done, returning HTTP response")
		//log.Printf("%s Leader response: %s", txId, string(resp))
	}

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Write(resp)
}

/*
func (s *Server) readHandler(w http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	value := s.db.Get(vars["key"])
	w.Write([]byte(value))
}

func (s *Server) writeHandler(w http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)

	// Read the value from the POST body.
	b, err := ioutil.ReadAll(req.Body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	value := string(b)

	// Execute the command against the Raft server.
	_, err = s.raftServer.Do(command.NewWriteCommand(vars["key"], value))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
	}
}
*/