package main

import (
	"fmt"
	zk "launchpad.net/gozk"
)

func main() {
	conn, session_watch, err := zk.Dial("192.168.64.87:2181", 5e9)
	if err != nil {
		fmt.Println("Couldn't connect: " + err.Error())
	}
	defer conn.Close()

	// Wait for zookeeper to connect
	event := <-session_watch
	watches := make(chan zk.Event)
	if event.State == zk.STATE_CONNECTED {
		fmt.Println("ZK connected")
	} else {
		fmt.Println("A different event: ", event.State)
	}

	// Setup watches
	fmt.Println("Going to do zk ops")
	_, watch, _ := conn.ExistsW("/andrew")
	go addWatchToAggregate(&watches, watch)
	_, _, watch, _ = conn.GetW("/data")
	go addWatchToAggregate(&watches, watch)

	for {
		event := <- watches
		fmt.Println("got watch...", event)
		if event.Path == "/andrew" {
			_, watch, _ = conn.ExistsW("/andrew")
			go addWatchToAggregate(&watches, watch)
		} else if event.Path == "/data" {
			_, _, watch, _ = conn.GetW("/data")
			go addWatchToAggregate(&watches, watch)
		}
	}
}

func addWatchToAggregate(watches *chan zk.Event, c <-chan zk.Event) {
	for s:= range c {
		*watches <- s
	}
}
