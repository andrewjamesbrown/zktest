package main

import (
	"fmt"
	zk "launchpad.net/gozk"
	"sync"
	"time"
)

type Watches struct {
	dataWatch  bool
	childWatch bool
}

var (
	mapLock sync.RWMutex
)

func main() {
	fmt.Println("Starting...")
	for { 
		conn, session_watch, err := zk.Dial("localhost:2181", 5e9)
		if err != nil {
			fmt.Println("Couldn't connect: " + err.Error())
		}
		defer conn.Close()


		fmt.Println ("Waiting...")
		// Wait for connection.
		// for {
			event := <-session_watch
			if event.State != zk.STATE_CONNECTED {
				println("Couldn't connect")
				return
			} else if event.State == zk.STATE_CONNECTED {
				println("Connected!")
			} else {
				println("A different event")
			}
		// }
	}
/*
	if watch != nil {
		for {
			_, ok := <-watch
			if !ok {
				break
			}
		}
	}
*/

/*
	watchMap := make(map[string]*Watches)
	for {
		// data, stat, watch, err := zk.GetW("/services")
		children, _, err := conn.Children("/ajb")
		fmt.Println(children)
		children, _, watch, err := conn.ChildrenW("/ajb/services")
		for _, element := range children { // Get list of services
			znode := "/ajb/services/" + element
			mapLock.RLock()
			mapState := watchMap[znode]
			mapLock.RUnlock()
			if mapState == nil {
				mapLock.Lock()
				watchMap[znode] = &Watches{false, false}
				mapLock.Unlock()
			}
			go getInstance(*conn, watchMap, znode)
			mapLock.RLock()
			mapState = watchMap[znode+"/instances"]
			mapLock.RUnlock()
			if mapState == nil {
				mapLock.Lock()
				watchMap[znode+"/instances"] = &Watches{false, false}
				mapLock.Unlock()
			}
			go getServiceInstances(*conn, watchMap, znode+"/instances")
		}
		if err != nil {
			fmt.Println("Error in get")
			time.Sleep(500 * time.Millisecond)
			continue
		}
		select {
		case <-watch:
		}
	}
*/
}

func getInstance(conn zk.Conn, watchMap map[string]*Watches, znode string) {
	mapLock.RLock()
	dataWatch := watchMap[znode].dataWatch
	mapLock.RUnlock()
	if dataWatch {
		return // we're already running, and we don't need two copies of ourselves.
	}
	for {
		mapLock.Lock()
		watchMap[znode].dataWatch = true
		mapLock.Unlock()
		data, _, watch, err := conn.GetW(znode)
		if err != nil {
			if zk.IsError(err, zk.ZNONODE) {
				mapLock.Lock()
				watchMap[znode].dataWatch = false
				mapLock.Unlock()
				return
			}
			fmt.Println(err)
			fmt.Println("Error in get: " + znode)
			time.Sleep(500 * time.Millisecond)
			continue
		}
		fmt.Println("<getInstance> Znode " + znode + ": " + data)
		select {
		case <-watch:
		}
	}
}

func getServiceInstances(conn zk.Conn, watchMap map[string]*Watches, znode string) {
	mapLock.RLock()
	childWatch := watchMap[znode].childWatch
	mapLock.RUnlock()
	if childWatch {
		return // we're already running, and don't need two copies of ourselves
	}
	for {
		mapLock.Lock()
		watchMap[znode].childWatch = true
		mapLock.Unlock()
		children, _, watch, err := conn.ChildrenW(znode)
		if err != nil {
			if zk.IsError(err, zk.ZNONODE) {
				// Someone created a new service, but the
				//   /services/:name/instances znode hasn't yet been created.
				fmt.Println("No znode " + znode)
				_, watch, err = conn.ExistsW(znode)
			} else {
				fmt.Println(err)
				fmt.Println("Error in getChildren: (" + znode + ") ")
				time.Sleep(500 * time.Millisecond)
				continue
			}
		}
		fmt.Println("<getServiceInstances> Znode " + znode + ": ")
		for _, element := range children {
			child := znode + "/" + element
			mapLock.RLock()
			tmp := watchMap[child]
			mapLock.RUnlock()
			if tmp == nil {
				mapLock.Lock()
				watchMap[child] = &Watches{false, false}
				mapLock.Unlock()
			}
			fmt.Println(child)
			go getInstance(conn, watchMap, child)
		}
		select {
		// TODO: analyze whether we need a separate watch channel
		//         for Exists() vs Children()
		case <-watch:
		}
	}
}
