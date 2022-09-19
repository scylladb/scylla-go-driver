package examples

import (
	"context"
	"fmt"
	"time"

	"github.com/scylladb/scylla-go-driver"
)

func Basic() error {
	ctx := context.Background()

	cfg := scylla.DefaultSessionConfig("exampleks", "192.168.100.100")
	session, err := scylla.NewSession(ctx, cfg)
	if err != nil {
		return err
	}
	defer session.Close()

	requestCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	q, err := session.Prepare(requestCtx, "SELECT id, name FROM exampleks.names WHERE id=?")
	if err != nil {
		return err
	}

	res, err := q.BindInt64(0, 64).Exec(requestCtx)
	if err != nil {
		return err
	}

	if len(res.Rows) == 0 {
		return fmt.Errorf("no rows found :(")
	}

	name, err := res.Rows[0][1].AsText()
	if err != nil {
		return err
	}
	fmt.Println(name)
	return nil
}
