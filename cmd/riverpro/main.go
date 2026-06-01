package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/divyam234/riverpro/driver/riverpropgxv5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river/rivermigrate"
)

const usage = `riverpro is the River Pro command line interface.

Commands:
  migrate-up    Apply River migrations. Defaults to the main line; use --line pro for Pro.
  migrate-down  Roll back River migrations. Defaults to the main line; use --line pro for Pro.
  migrate-get   Print SQL for a bundled migration version.

Examples:
  riverpro migrate-up --database-url "$DATABASE_URL"
  riverpro migrate-up --database-url "$DATABASE_URL" --line pro
  riverpro migrate-get --line pro --version 1 --up
`

func main() {
	if err := run(context.Background(), os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 || args[0] == "help" || args[0] == "--help" || args[0] == "-h" {
		_, _ = fmt.Fprint(stdout, usage)
		return nil
	}

	switch args[0] {
	case "migrate-up":
		return runMigrate(ctx, rivermigrate.DirectionUp, args[1:], stdout, stderr)
	case "migrate-down":
		return runMigrate(ctx, rivermigrate.DirectionDown, args[1:], stdout, stderr)
	case "migrate-get":
		return runMigrateGet(args[1:], stdout)
	default:
		return fmt.Errorf("unknown command %q\n\n%s", args[0], usage)
	}
}

func runMigrate(ctx context.Context, direction rivermigrate.Direction, args []string, stdout, _ io.Writer) error {
	fs := flag.NewFlagSet("migrate-"+string(direction), flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	var databaseURL, line, schema string
	var dryRun bool
	var maxSteps, targetVersion int
	fs.StringVar(&databaseURL, "database-url", "", "PostgreSQL database URL. Defaults to DATABASE_URL.")
	fs.StringVar(&line, "line", "", "Migration line. Empty defaults to the main River line; use pro for River Pro.")
	fs.StringVar(&schema, "schema", "", "PostgreSQL schema to migrate.")
	fs.BoolVar(&dryRun, "dry-run", false, "Print migration SQL without applying it.")
	fs.IntVar(&maxSteps, "max-steps", 0, "Maximum migration steps. Zero uses River's default.")
	fs.IntVar(&targetVersion, "target-version", 0, "Target migration version. Zero uses River's default.")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if databaseURL == "" {
		databaseURL = os.Getenv("DATABASE_URL")
	}
	if databaseURL == "" {
		return errors.New("--database-url or DATABASE_URL is required")
	}

	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return fmt.Errorf("connect database: %w", err)
	}
	defer pool.Close()

	migrator, err := rivermigrate.New(riverpropgxv5.New(pool), &rivermigrate.Config{Line: line, Schema: schema})
	if err != nil {
		return err
	}
	res, err := migrator.Migrate(ctx, direction, &rivermigrate.MigrateOpts{DryRun: dryRun, MaxSteps: maxSteps, TargetVersion: targetVersion})
	if err != nil {
		return err
	}
	if len(res.Versions) == 0 {
		_, _ = fmt.Fprintln(stdout, "No migrations to apply.")
		return nil
	}
	for _, version := range res.Versions {
		if dryRun {
			_, _ = fmt.Fprintln(stdout, strings.TrimSpace(version.SQL))
			continue
		}
		_, _ = fmt.Fprintf(stdout, "Migrated [%s] line %q version %d: %s\n", strings.ToUpper(string(res.Direction)), effectiveLine(line), version.Version, version.Name)
	}
	return nil
}

func runMigrateGet(args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("migrate-get", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	var line string
	var version int
	var up, down bool
	fs.StringVar(&line, "line", "", "Migration line. Empty defaults to the main River line; use pro for River Pro.")
	fs.IntVar(&version, "version", 0, "Migration version to print.")
	fs.BoolVar(&up, "up", false, "Print the up SQL.")
	fs.BoolVar(&down, "down", false, "Print the down SQL.")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if version <= 0 {
		return errors.New("--version must be greater than zero")
	}
	if up == down {
		return errors.New("exactly one of --up or --down is required")
	}
	migrator, err := rivermigrate.New(riverpropgxv5.New(nil), &rivermigrate.Config{Line: line})
	if err != nil {
		return err
	}
	migration, err := migrator.GetVersion(version)
	if err != nil {
		return err
	}
	if up {
		_, _ = fmt.Fprint(stdout, migration.SQLUp)
	} else {
		_, _ = fmt.Fprint(stdout, migration.SQLDown)
	}
	return nil
}

func effectiveLine(line string) string {
	if line == "" {
		return "main"
	}
	return line
}
