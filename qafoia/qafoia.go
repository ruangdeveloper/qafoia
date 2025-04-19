package qafoia

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"time"
)

type Qafoia interface {
	New(config *Config) (*Qafoia, error)
	Register(migrations ...Migration) error
	Create(name string) error
	Migrate(ctx context.Context) error
	Fresh(ctx context.Context) error
	Reset(ctx context.Context) error
	Rollback(ctx context.Context, step int) error
	Clean(ctx context.Context) error
	List(ctx context.Context) (RegisteredMigrationList, error)
}

type qafoiaImplementation struct {
	driver            Driver
	migrationFilesDir string
	debugSql          bool
	migrations        map[string]Migration
	mu                sync.Mutex
}

func New(config *Config) (*qafoiaImplementation, error) {
	if config == nil {
		return nil, ErrConfigNotProvided
	}
	if config.Driver == nil {
		return nil, ErrDriverNotProvided
	}

	// Set default values if not provided
	if config.MigrationFilesDir == "" {
		config.MigrationFilesDir = "migrations"
	}
	if config.MigrationTableName == "" {
		config.MigrationTableName = "migrations"
	}

	// Validate table name
	if _, err := sanitizeTableName(config.MigrationTableName); err != nil {
		return nil, fmt.Errorf("invalid migration table name: %w", err)
	}

	// Check if migration directory exists
	if !migrationDirExists(config.MigrationFilesDir) {
		return nil, fmt.Errorf("migration directory %q does not exist", config.MigrationFilesDir)
	}

	// Apply configuration
	config.Driver.SetMigrationTableName(config.MigrationTableName)

	return &qafoiaImplementation{
		driver:            config.Driver,
		migrationFilesDir: config.MigrationFilesDir,
		debugSql:          config.DebugSql,
		migrations:        make(map[string]Migration),
	}, nil
}

func (q *qafoiaImplementation) Register(migrations ...Migration) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	for _, migration := range migrations {
		name := migration.Name()
		if name == "" {
			return ErrMigrationNameNotProvided
		}
		if _, exists := q.migrations[name]; exists {
			return fmt.Errorf("migration %s registered more than once", name)
		}
		q.migrations[name] = migration
	}

	return nil
}

func (q *qafoiaImplementation) Create(fileName string) error {
	if fileName == "" {
		return ErrMigrationNameNotProvided
	}

	migrationName, err := sanitizeMigrationName(fileName)
	if err != nil {
		return err
	}

	migrationName = fmt.Sprintf("%s_%s", time.Now().Format("20060102150405"), migrationName)
	migrationFileName := fmt.Sprintf("%s/%s.go", q.migrationFilesDir, migrationName)

	if fileExists(migrationFileName) {
		return ErrMigrationFileAlreadyExists
	}

	template, err := migrationFileTemplate(getPackageNameFromMigrationDir(q.migrationFilesDir), migrationName)
	if err != nil {
		return err
	}

	err = os.WriteFile(migrationFileName, []byte(template), 0644)
	if err != nil {
		return err
	}
	log.Printf("migration file created: %s\n", migrationFileName)

	return nil
}

func (q *qafoiaImplementation) Migrate(ctx context.Context) error {
	if err := q.driver.CreateMigrationsTable(ctx); err != nil {
		return err
	}

	executedMigrations, err := q.driver.GetExecutedMigrations(ctx, false)
	if err != nil {
		return err
	}

	executedMap := make(map[string]struct{}, len(executedMigrations))
	for _, m := range executedMigrations {
		executedMap[m.Name] = struct{}{}
	}

	migrationsToApply := make([]Migration, 0, len(q.migrations))
	for _, name := range getSortedMigrationName(q.migrations) {
		migration := q.migrations[name]
		if _, found := executedMap[migration.Name()]; !found {
			migrationsToApply = append(migrationsToApply, migration)
		}
	}

	if len(migrationsToApply) == 0 {
		log.Println("✅ No migrations to run")
		return nil
	}

	log.Printf("🚀 Applying %d migration(s)...\n", len(migrationsToApply))

	return q.driver.ApplyMigrations(
		ctx,
		migrationsToApply,
		func(migration *Migration) {
			name := (*migration).Name()
			log.Printf("📦 Migrating: %s\n", name)
			if q.debugSql {
				log.Printf("🧾 Running SQL:\n%s\n", (*migration).UpScript())
			}
		},
		func(migration *Migration) {
			log.Printf("✅ Migrated: %s\n", (*migration).Name())
		},
		func(migration *Migration, err error) {
			log.Printf("❌ Migration failed: %s - %s\n", (*migration).Name(), err)
		},
	)
}

func (q *qafoiaImplementation) Fresh(ctx context.Context) error {
	log.Println("🧹 Cleaning database...")

	if err := q.driver.CleanDatabase(ctx); err != nil {
		return fmt.Errorf("failed to clean database: %w", err)
	}

	log.Println("🚀 Running fresh migrations...")

	if err := q.Migrate(ctx); err != nil {
		return fmt.Errorf("failed to run migrations after cleaning: %w", err)
	}

	log.Println("✅ Fresh migration completed successfully")
	return nil
}

func (q *qafoiaImplementation) Reset(ctx context.Context) error {
	executedMigrations, err := q.driver.GetExecutedMigrations(ctx, true)
	if err != nil {
		return fmt.Errorf("failed to get executed migrations: %w", err)
	}

	if len(executedMigrations) == 0 {
		log.Println("✅ No migrations to reset")
		return nil
	}

	log.Printf("🔁 Resetting %d executed migration(s)...\n", len(executedMigrations))

	if err := q.Rollback(ctx, len(executedMigrations)); err != nil {
		return fmt.Errorf("rollback failed during reset: %w", err)
	}

	if err := q.Migrate(ctx); err != nil {
		return fmt.Errorf("migration failed during reset: %w", err)
	}

	log.Println("✅ Migration reset completed successfully")
	return nil
}

func (q *qafoiaImplementation) Rollback(ctx context.Context, step int) error {
	if step <= 0 {
		return ErrInvalidRollbackStep
	}

	executedMigrations, err := q.driver.GetExecutedMigrations(ctx, true)
	if err != nil {
		return err
	}

	if len(executedMigrations) == 0 {
		log.Println("✅ No migrations to rollback")
		return nil
	}

	if step > len(executedMigrations) {
		step = len(executedMigrations)
	}

	// 🔥 Build a map for faster lookup
	migrationMap := make(map[string]Migration, len(q.migrations))
	for _, m := range q.migrations {
		migrationMap[m.Name()] = m
	}

	migrationsToRollback := make([]Migration, 0, step)
	for i := range step {
		executedMigration := executedMigrations[i]
		if migration, found := migrationMap[executedMigration.Name]; found {
			migrationsToRollback = append(migrationsToRollback, migration)
		} else {
			log.Printf("⚠️  Migration not found for: %s\n", executedMigration.Name)
		}
	}

	if len(migrationsToRollback) == 0 {
		log.Println("✅ No migrations to rollback")
		return nil
	}

	log.Printf("🔁 Rolling back %d migration(s)...\n", len(migrationsToRollback))

	return q.driver.UnapplyMigrations(
		ctx,
		migrationsToRollback,
		func(migration *Migration) {
			name := (*migration).Name()
			log.Printf("🔄 Rolling back: %s\n", name)
			if q.debugSql {
				log.Printf("🧾 Running SQL:\n%s\n", (*migration).DownScript())
			}
		},
		func(migration *Migration) {
			log.Printf("✅ Rolled back: %s\n", (*migration).Name())
		},
		func(migration *Migration, err error) {
			log.Printf("❌ Rollback failed: %s - %s\n", (*migration).Name(), err)
		},
	)
}

func (q *qafoiaImplementation) Clean(ctx context.Context) error {
	log.Println("🧹 Cleaning database...")

	if err := q.driver.CleanDatabase(ctx); err != nil {
		return fmt.Errorf("failed to clean database: %w", err)
	}

	log.Println("✅ Database cleaned successfully")
	return nil
}

func (q *qafoiaImplementation) List(ctx context.Context) (RegisteredMigrationList, error) {
	if err := q.driver.CreateMigrationsTable(ctx); err != nil {
		return nil, err
	}

	executedMigrations, err := q.driver.GetExecutedMigrations(ctx, false)
	if err != nil {
		return nil, err
	}

	executedMap := make(map[string]bool, len(executedMigrations))
	for _, m := range executedMigrations {
		executedMap[m.Name] = true
	}

	registeredMigrations := make(RegisteredMigrationList, 0, len(q.migrations))

	for _, k := range getSortedMigrationName(q.migrations) {
		migration := q.migrations[k]
		name := migration.Name()

		registeredMigrations = append(registeredMigrations, RegisteredMigration{
			Name:       name,
			UpScript:   migration.UpScript(),
			DownScript: migration.DownScript(),
			IsExecuted: executedMap[name],
		})
	}

	return registeredMigrations, nil
}
