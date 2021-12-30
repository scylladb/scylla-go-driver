package main

import (
	"fmt"
	"scylla-go-driver/transport"
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
				fmt.Printf("%d ", int(value[0])<<24 |
					int(value[1])<<16 |
					int(value[2])<<8 |
					int(value[3]))
			} else {
				fmt.Printf("%s ", value)
			}
		}
		fmt.Println()
	}
}
