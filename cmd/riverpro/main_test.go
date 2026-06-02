package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestMigrateGetPro(t *testing.T) {
	var out bytes.Buffer
	err := run(context.Background(), []string{"migrate-get", "--line", "pro", "--version", "1", "--up"}, &out, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("run migrate-get: %v", err)
	}
	if !strings.Contains(out.String(), "CREATE TABLE IF NOT EXISTS") || !strings.Contains(out.String(), "river_workflow") {
		t.Fatalf("unexpected pro migration SQL: %s", out.String())
	}
}

func TestMigrateGetDeprecatedLinesRemoved(t *testing.T) {
	var out bytes.Buffer
	err := run(context.Background(), []string{"migrate-get", "--line", "sequence", "--version", "1", "--up"}, &out, &bytes.Buffer{})
	if err == nil {
		t.Fatal("expected deprecated sequence line to fail")
	}
	if !strings.Contains(err.Error(), "migration line does not exist") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestMigrateUpMainThenPro(t *testing.T) {
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer pool.Close()

	schema := fmt.Sprintf("riverpro_cli_%d", time.Now().UnixNano())
	if _, err := pool.Exec(ctx, `CREATE SCHEMA `+schema); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	defer pool.Exec(context.Background(), `DROP SCHEMA IF EXISTS `+schema+` CASCADE`)

	for _, args := range [][]string{
		{"migrate-up", "--database-url", databaseURL, "--schema", schema},
		{"migrate-up", "--database-url", databaseURL, "--schema", schema, "--line", "pro"},
	} {
		var out bytes.Buffer
		if err := run(ctx, args, &out, &bytes.Buffer{}); err != nil {
			t.Fatalf("run %v: %v\n%s", args, err, out.String())
		}
	}

	var mainCount, proCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM `+schema+`.river_migration WHERE line = 'main'`).Scan(&mainCount); err != nil {
		t.Fatalf("query main migrations: %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM `+schema+`.river_migration WHERE line = 'pro'`).Scan(&proCount); err != nil {
		t.Fatalf("query pro migrations: %v", err)
	}
	if mainCount == 0 || proCount != 2 {
		t.Fatalf("unexpected migration counts: main=%d pro=%d", mainCount, proCount)
	}
}
