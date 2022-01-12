package main

import (
	"fmt"
	"scylla-go-driver/frame"
	"scylla-go-driver/transport"
	"time"
)

// TODO Delete this package.
// This file is temporary! Only for purpose of showing connection.

func main() {

	// Run scylla on port 9999
	// docker exec -it scylla cqlsh
	// CREATE KEYSPACE mykeyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};
	// use mykeyspace;
	// CREATE TABLE users (user_id int, fname text, lname text, PRIMARY KEY((user_id)));
	// insert into users(user_id, fname, lname) values (1, 'rick', 'sanchez');
	// insert into users(user_id, fname, lname) values (4, 'rust', 'cohle');

	session, err := transport.MakeSession(":9999")
	if err != nil {
		panic(err)
	}

	rows := session.Query("SELECT * FROM mykeyspace.users").Await().Rows
	for _, row := range rows {
		for i, value := range row {
			if i == 0 {
				fmt.Printf("%d ", int(value[0])<<24|
					int(value[1])<<16|
					int(value[2])<<8|
					int(value[3]))
			} else {
				fmt.Printf("%s ", value)
			}
		}
		fmt.Println()
	}

	control := transport.NewControlConn(":9999", &session)
	if res := control.RegisterEvents(); (<-res).Header.Opcode != frame.OpReady {
		panic("invalid response frame")
	}

	pi, _ := control.DiscoverTopology()
	fmt.Printf("mapa: %v\n", pi)
	hp, _ := control.InitHostPool(":9999")
	fmt.Printf("pool: %v\n", hp)
	hp, _ = control.InitHostPool(":9998")
	fmt.Printf("pool: %v\n", hp)

	// Now you can type "SELECT * FROM system.clients" in cqlsh to see, that connection pool covers all the shards.
	time.Sleep(10000000000)
}
