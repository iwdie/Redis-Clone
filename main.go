package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/iwdie/Redis-Clone/aof"
	"github.com/iwdie/Redis-Clone/server"
	"github.com/iwdie/Redis-Clone/store"
)

func main() {
	addr := flag.String("addr", ":6379", "address to listen on")
	aofPath := flag.String("aof", "appendonly.aof", "append-only file path (empty to disable persistence)")
	flag.Parse()

	st := store.New()
	st.StartJanitor(100 * time.Millisecond)
	defer st.StopJanitor()

	srv := server.New(*addr, st)

	if *aofPath != "" {
		n, err := srv.LoadAOF(*aofPath)
		if err != nil {
			log.Fatalf("loading AOF: %v", err)
		}
		log.Printf("replayed %d commands from %s", n, *aofPath)

		a, err := aof.Open(*aofPath, time.Second)
		if err != nil {
			log.Fatalf("opening AOF: %v", err)
		}
		defer a.Close()
		srv.SetAOF(a)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sig
		log.Println("shutting down...")
		srv.Shutdown()
	}()

	if err := srv.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
	log.Println("bye")
}
