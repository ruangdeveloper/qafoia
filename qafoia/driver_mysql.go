package qafoia

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

type MySqlDriver struct {
	db                 *sql.DB
	migrationTableName string
}

func NewMySqlDriver(
	host string,
	port string,
	user string,
	password string,
	database string,
	charset string,
) (*MySqlDriver, error) {
	if charset == "" {
		charset = "utf8mb4"
	}
	dsn := fmt.Sprintf(
		"%s:%s@tcp(%s:%s)/%s?charset=%s&parseTime=True&loc=Local",
		user,
		password,
		host,
		port,
		database,
		charset,
	)

	db, err := sql.Open("mysql", dsn)

	if err != nil {
		return nil, err
	}

	if err := db.Ping(); err != nil {
		return nil, err
	}

	return &MySqlDriver{
		db:                 db,
		migrationTableName: "migrations",
	}, nil
}

func (m *MySqlDriver) Close() error {
	if m.db != nil {
		if err := m.db.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (m *MySqlDriver) SetMigrationTableName(name string) {
	if name == "" {
		name = "migrations"
	}
	m.migrationTableName = name
}

func (m *MySqlDriver) CreateMigrationsTable(ctx context.Context) error {
	query := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			name VARCHAR(255) PRIMARY KEY NOT NULL,
			executed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)
	`, m.migrationTableName)
	_, err := m.db.ExecContext(ctx, query)

	if err != nil {
		return err
	}
	return nil
}

func (m *MySqlDriver) GetExecutedMigrations(ctx context.Context, reverse bool) ([]ExecutedMigration, error) {
	order := "ASC"
	if reverse {
		order = "DESC"
	}

	query := fmt.Sprintf(`SELECT name, executed_at FROM %s ORDER BY name %s`, m.migrationTableName, order)

	rows, err := m.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	migrations := []ExecutedMigration{}

	for rows.Next() {
		var name string
		var executedAt time.Time

		if err := rows.Scan(&name, &executedAt); err != nil {
			return nil, err
		}

		migrations = append(migrations, ExecutedMigration{
			Name:       name,
			ExecutedAt: executedAt,
		})
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return migrations, nil
}

func (m *MySqlDriver) CleanDatabase(ctx context.Context) error {
	// Disable foreign key checks to drop tables in any order
	_, err := m.db.ExecContext(ctx, `SET FOREIGN_KEY_CHECKS = 0;`)
	if err != nil {
		return fmt.Errorf("failed to disable FK checks: %w", err)
	}

	// Query all table names in the current database
	rows, err := m.db.QueryContext(ctx, `
		SELECT table_name 
		FROM information_schema.tables 
		WHERE table_schema = DATABASE();
	`)
	if err != nil {
		return fmt.Errorf("failed to query tables: %w", err)
	}
	defer rows.Close()

	var tableNames []string
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			return fmt.Errorf("failed to scan table name: %w", err)
		}
		tableNames = append(tableNames, fmt.Sprintf("`%s`", table))
	}

	// If no tables, skip drop
	if len(tableNames) == 0 {
		return nil
	}

	// Drop all tables in a single statement
	dropSQL := fmt.Sprintf("DROP TABLE %s;", strings.Join(tableNames, ", "))
	_, err = m.db.ExecContext(ctx, dropSQL)
	if err != nil {
		return fmt.Errorf("failed to drop tables: %w", err)
	}

	// Re-enable foreign key checks
	_, err = m.db.ExecContext(ctx, `SET FOREIGN_KEY_CHECKS = 1;`)
	if err != nil {
		return fmt.Errorf("failed to re-enable FK checks: %w", err)
	}

	log.Println("all tables dropped successfully")
	return nil
}

func (m *MySqlDriver) ApplyMigrations(ctx context.Context, migrations []Migration, onRunning func(migration *Migration), onSuccess func(migration *Migration), onFailed func(migration *Migration, err error)) error {
	for _, migration := range migrations {
		if onRunning != nil {
			onRunning(&migration)
		}
		err := m.executeMigrationSQL(ctx, []byte(migration.UpScript()))
		if err != nil {
			if onFailed != nil {
				onFailed(&migration, err)
			}
			return err
		}
		err = m.insertExecutedMigration(ctx, migration.Name(), time.Now())
		if err != nil {
			if onFailed != nil {
				onFailed(&migration, err)
			}
			return err
		}
		if onSuccess != nil {
			onSuccess(&migration)
		}
	}

	return nil
}

func (m *MySqlDriver) UnapplyMigrations(ctx context.Context, migrations []Migration, onRunning func(migration *Migration), onSuccess func(migration *Migration), onFailed func(migration *Migration, err error)) error {
	for _, migration := range migrations {
		if onRunning != nil {
			onRunning(&migration)
		}
		err := m.executeMigrationSQL(ctx, []byte(migration.DownScript()))
		if err != nil {
			if onFailed != nil {
				onFailed(&migration, err)
			}
			return err
		}
		err = m.removeExecutedMigration(ctx, migration.Name())
		if err != nil {
			if onFailed != nil {
				onFailed(&migration, err)
			}
			return err
		}
		if onSuccess != nil {
			onSuccess(&migration)
		}
	}
	return nil
}

func (m *MySqlDriver) executeMigrationSQL(ctx context.Context, sqlBytes []byte) error {
	sql := string(sqlBytes)

	if sql == "" {
		return nil
	}

	_, err := m.db.ExecContext(ctx, string(sqlBytes))

	if err != nil {
		return err
	}
	return nil
}

func (m *MySqlDriver) insertExecutedMigration(ctx context.Context, name string, executedAt time.Time) error {
	query := fmt.Sprintf(`INSERT INTO %s (name, executed_at) VALUES (?, ?)`, m.migrationTableName)
	_, err := m.db.ExecContext(ctx, query, name, executedAt)

	if err != nil {
		return err
	}
	return nil
}

func (m *MySqlDriver) removeExecutedMigration(ctx context.Context, name string) error {
	query := fmt.Sprintf(`DELETE FROM %s WHERE name = ?`, m.migrationTableName)
	_, err := m.db.ExecContext(ctx, query, name)

	if err != nil {
		return err
	}
	return nil
}
