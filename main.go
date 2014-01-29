package main

import (
	"flag"
	"fmt"
	"github.com/coreos/raft"
	"stripe-ctf.com/sqlcluster/command"
	"stripe-ctf.com/sqlcluster/server"
	"stripe-ctf.com/sqlcluster/log"
	"math/rand"
	"os"
	"os/signal"
	"io/ioutil"
	"syscall"
	"time"
)

var verbose bool
var trace bool
var debug bool
var listen string
var join string
var directory string

func init() {
	flag.BoolVar(&verbose, "v", false, "verbose logging")
	flag.BoolVar(&trace, "trace", false, "Raft trace debugging")
	flag.BoolVar(&debug, "debug", false, "Raft debugging")
	flag.StringVar(&listen, "l", "127.0.0.1:4000", "Socket to listen on (Unix or TCP)")
	flag.StringVar(&join, "join", "", "host:port of leader to join")
	flag.StringVar(&directory, "d", "", "Storage directory")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [arguments] <data-path> \n", os.Args[0])
		flag.PrintDefaults()
	}
}

func main() {
	log.SetFlags(0)
	flag.Parse()
	if verbose {
		log.Print("Verbose logging enabled.")
	}
	if trace {
		raft.SetLogLevel(raft.Trace)
		log.Print("Raft trace debugging enabled.")
	} else if debug {
		raft.SetLogLevel(raft.Debug)
		log.Print("Raft debugging enabled.")
	}

	if directory == "" {
		var err error
		directory, err = ioutil.TempDir("/tmp", "node")
		if err != nil {
			log.Fatalf("Could not create temporary base directory: %s", err)
		}
		defer os.RemoveAll(directory)

		log.Printf("Storing state in tmpdir %s", directory)
	} else {
		if err := os.MkdirAll(directory, os.ModeDir|0755); err != nil {
			log.Fatalf("Error while creating storage directory: %s", err)
		}
	}

	log.Printf("Changing directory to %s", directory)
	if err := os.Chdir(directory); err != nil {
		log.Fatalf("Error while changing to storage directory: %s", err)
	}

	rand.Seed(time.Now().UnixNano())

	// Setup commands.
	raft.RegisterCommand(&command.SQLCommand{})

	// Start the server
	go func() {
		s, err := server.New(directory, listen)
		if err != nil {
			log.Fatal(err)
		}

		if err := s.ListenAndServe(join); err != nil {
			log.Fatal(err)
		}
	}()

	// Exit cleanly so we can remove the tmpdir
	sigchan := make(chan os.Signal)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
	<-sigchan

	log.Printf("Bye!")
}
