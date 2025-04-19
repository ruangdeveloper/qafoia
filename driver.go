package qafoia

import (
	"context"
)

type Driver interface {
	SetMigrationTableName(name string)
	CreateMigrationsTable(ctx context.Context) error
	GetExecutedMigrations(ctx context.Context, reverse bool) ([]ExecutedMigration, error)
	CleanDatabase(ctx context.Context) error
	ApplyMigrations(ctx context.Context, migrations []Migration, onRunning func(migration *Migration), onSuccess func(migration *Migration), onFailed func(migration *Migration, err error)) error
	UnapplyMigrations(ctx context.Context, migrations []Migration, onRunning func(migration *Migration), onSuccess func(migration *Migration), onFailed func(migration *Migration, err error)) error
	Close() error
}
