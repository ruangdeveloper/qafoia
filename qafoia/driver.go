package qafoia

import (
	"context"
)

type Driver interface {
	SetMigrationTableName(name string)
	CreateMigrationsTable(ctx context.Context) error
	GetExecutedMigrations(ctx context.Context, reverse bool) ([]ExecutedMigration, error)
	CleanDatabase(ctx context.Context) error
	ApplyMigrations(ctx context.Context, migrations MigrationFiles, onRunning func(migration *MigrationFile), onSuccess func(migration *MigrationFile), onFailed func(migration *MigrationFile, err error)) error
	UnapplyMigrations(ctx context.Context, migrations MigrationFiles, onRunning func(migration *MigrationFile), onSuccess func(migration *MigrationFile), onFailed func(migration *MigrationFile, err error)) error
	Close() error
}
