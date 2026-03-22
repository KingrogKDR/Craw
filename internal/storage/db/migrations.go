package db

import (
	"context"
	"os"
)

func RunMigrations() error {
	data, err := os.ReadFile("schema.sql")
	if err != nil {
		return err
	}

	_, err = Pool.Exec(context.Background(), string(data))
	return err
}
