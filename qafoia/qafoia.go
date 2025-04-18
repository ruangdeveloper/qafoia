package qafoia

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"
)

type Qafoia interface {
	New(config *Config) (*Qafoia, error)
	Create(name string) error
	Migrate(ctx context.Context) error
	Fresh(ctx context.Context) error
	Reset(ctx context.Context) error
	Rollback(ctx context.Context, step int) error
	Clean(ctx context.Context) error
	List(ctx context.Context) (MigrationFiles, error)
}

type qafoiaImplementation struct {
	driver            Driver
	migrationFilesDir string
	debugSql          bool
}

func New(config *Config) (*qafoiaImplementation, error) {
	if config == nil {
		return nil, ErrConfigNotProvided
	}

	if config.Driver == nil {
		return nil, ErrDriverNotProvided
	}

	if config.MigrationFilesDir == "" {
		config.MigrationFilesDir = "migrations"
	}

	if config.MigrationTableName == "" {
		config.MigrationTableName = "migrations"
	}

	_, err := sanitizeTableName(config.MigrationTableName)
	if err != nil {
		return nil, err
	}

	if !migrationDirExists(config.MigrationFilesDir) {
		return nil, ErrMigrationDirNotExists
	}

	config.Driver.SetMigrationTableName(config.MigrationTableName)

	qafoia := &qafoiaImplementation{
		driver:            config.Driver,
		migrationFilesDir: config.MigrationFilesDir,
		debugSql:          config.DebugSql,
	}

	return qafoia, nil
}

func (q *qafoiaImplementation) Create(fileName string) error {
	if fileName == "" {
		return ErrMigrationNameNotProvided
	}

	fileName, err := sanitizeMigrationName(fileName)
	if err != nil {
		return err
	}

	timestamp := time.Now().Format("20060102150405")
	name := fmt.Sprintf("%s/%s_%s", q.migrationFilesDir, timestamp, fileName)
	upName := fmt.Sprintf("%s.up.sql", name)
	downName := fmt.Sprintf("%s.down.sql", name)

	if fileExists(upName) || fileExists(downName) {
		return ErrMigrationFileAlreadyExists
	}

	_, err = os.Create(upName)
	if err != nil {
		return err
	}

	_, err = os.Create(downName)
	if err != nil {
		if fileExists(upName) {
			os.Remove(upName)
		}
	}

	log.Printf("migration file created: %s\n", upName)
	log.Printf("migration file created: %s\n", downName)

	return nil
}

func (q *qafoiaImplementation) Migrate(ctx context.Context) error {
	err := q.driver.CreateMigrationsTable(ctx)
	if err != nil {
		return err
	}

	migrationFiles, err := collectMigrationFiles(q.migrationFilesDir)

	if err != nil {
		return err
	}

	executedMigrations, err := q.driver.GetExecutedMigrations(ctx, false)
	if err != nil {
		return err
	}

	var migrationsToApply MigrationFiles

	for _, migrationFile := range migrationFiles {
		_, found := findExecutedMigration(migrationFile.BaseName, executedMigrations)
		if !found {
			migrationsToApply = append(migrationsToApply, migrationFile)
		}
	}

	if len(migrationsToApply) == 0 {
		log.Println("no migrations to run")
	}

	return q.driver.ApplyMigrations(
		ctx,
		migrationsToApply,
		func(migration *MigrationFile) {
			log.Printf("migrating: %s\n", migration.BaseName)
			if q.debugSql {
				log.Printf("running SQL: %s\n", string(migration.UpSql))
			}
		},
		func(migration *MigrationFile) {
			log.Printf("migrated: %s\n", migration.BaseName)
		},
		func(migration *MigrationFile, err error) {
			log.Printf("migration failed: %s - %s\n", migration.BaseName, err.Error())
		},
	)
}

func (q *qafoiaImplementation) Fresh(ctx context.Context) error {
	log.Println("cleaning database")
	err := q.driver.CleanDatabase(ctx)
	if err != nil {
		return err
	}

	return q.Migrate(ctx)
}

func (q *qafoiaImplementation) Reset(ctx context.Context) error {
	executedMigrations, err := q.driver.GetExecutedMigrations(ctx, true)
	if err != nil {
		return err
	}

	if len(executedMigrations) == 0 {
		log.Println("no migrations to reset")
		return nil
	}

	err = q.Rollback(ctx, len(executedMigrations))
	if err != nil {
		return err
	}

	err = q.Migrate(ctx)
	if err != nil {
		return err
	}

	return nil
}

func (q *qafoiaImplementation) Rollback(ctx context.Context, step int) error {
	if step <= 0 {
		return ErrInvalidRollbackStep
	}

	migrationFiles, err := collectMigrationFiles(q.migrationFilesDir)
	if err != nil {
		return err
	}

	executedMigrations, err := q.driver.GetExecutedMigrations(ctx, true)
	if err != nil {
		return err
	}

	if len(executedMigrations) == 0 {
		log.Println("no migrations to rollback")
		return nil
	}

	if step > len(executedMigrations) {
		step = len(executedMigrations)
	}

	var migrationsToRollback MigrationFiles
	for i := range step {
		executedMigration := executedMigrations[i]
		migrationFile, found := findMigrationFile(executedMigration.Name, migrationFiles)
		if !found {
			log.Printf("migration file not found for: %s", executedMigration.Name)
			continue
		}

		migrationsToRollback = append(migrationsToRollback, *migrationFile)
	}

	if len(migrationsToRollback) == 0 {
		log.Println("no migrations to rollback")
	}

	return q.driver.UnapplyMigrations(
		ctx,
		migrationsToRollback,
		func(migration *MigrationFile) {
			log.Printf("rolling back: %s\n", migration.BaseName)
			if q.debugSql {
				log.Printf("running SQL: %s\n", string(migration.DownSql))
			}
		},
		func(migration *MigrationFile) {
			log.Printf("rolled back: %s\n", migration.BaseName)
		},
		func(migration *MigrationFile, err error) {
			log.Printf("rollback failed: %s - %s\n", migration.BaseName, err.Error())
		},
	)
}

func (q *qafoiaImplementation) Clean(ctx context.Context) error {
	log.Println("cleaning database")
	err := q.driver.CleanDatabase(ctx)
	if err != nil {
		return err
	}

	log.Println("database cleaned")
	return nil
}

func (q *qafoiaImplementation) List(ctx context.Context) (MigrationFiles, error) {
	err := q.driver.CreateMigrationsTable(ctx)
	if err != nil {
		return nil, err
	}

	migrationFiles, err := collectMigrationFiles(q.migrationFilesDir)
	if err != nil {
		return nil, err
	}

	executedMigrations, err := q.driver.GetExecutedMigrations(ctx, false)
	if err != nil {
		return nil, err
	}

	for i := range migrationFiles {
		_, found := findExecutedMigration(migrationFiles[i].BaseName, executedMigrations)
		if found {
			migrationFiles[i].IsExecuted = true
		}
	}

	return migrationFiles, nil
}
