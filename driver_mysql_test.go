package qafoia

import (
	"context"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
)

func TestNewMySqlDriver(t *testing.T) {
	// Create a mock database connection
	db, mock, err := sqlmock.New(
		sqlmock.MonitorPingsOption(true),
	)
	if err != nil {
		t.Fatalf("could not create mock db: %v", err)
	}
	defer db.Close()

	// Simulate a successful ping to the DB
	mock.ExpectPing().WillReturnError(nil)

	// Initialize MySqlDriver with the mock DB
	driver := &MySqlDriver{
		db:                 db,
		migrationTableName: "migrations",
	}

	// Test that the driver is initialized correctly
	assert.NotNil(t, driver)
}

func TestCreateMigrationsTable(t *testing.T) {
	// Create a mock database connection
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("could not create mock db: %v", err)
	}
	defer db.Close()

	// Simulate a successful table creation
	mock.ExpectExec("CREATE TABLE IF NOT EXISTS migrations").WillReturnResult(sqlmock.NewResult(1, 1))

	// Initialize MySqlDriver with the mock DB
	driver := &MySqlDriver{
		db:                 db,
		migrationTableName: "migrations",
	}

	// Call CreateMigrationsTable
	err = driver.CreateMigrationsTable(context.Background())
	assert.NoError(t, err)
}

func TestGetExecutedMigrations(t *testing.T) {
	// Create a mock database connection
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("could not create mock db: %v", err)
	}
	defer db.Close()

	// Simulate the query to fetch migrations
	rows := sqlmock.NewRows([]string{"name", "executed_at"}).
		AddRow("migration_1", time.Now()).
		AddRow("migration_2", time.Now())

	mock.ExpectQuery("SELECT name, executed_at FROM migrations").
		WillReturnRows(rows)

	// Initialize MySqlDriver with the mock DB
	driver := &MySqlDriver{
		db:                 db,
		migrationTableName: "migrations",
	}

	// Call GetExecutedMigrations
	migrations, err := driver.GetExecutedMigrations(context.Background(), false)
	assert.NoError(t, err)
	assert.Len(t, migrations, 2)
	assert.Equal(t, "migration_1", migrations[0].Name)
}
