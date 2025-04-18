package qafoia

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

type MySqlDriver struct {
	db *sql.DB
}

func NewMySqlDriver(
	host string,
	port string,
	user string,
	password string,
	database string,
) (*MySqlDriver, error) {
	dsn := fmt.Sprintf(
		"%s:%s@tcp(%s:%s)/%s?charset=utf8mb4&parseTime=True&loc=Local",
		user,
		password,
		host,
		port,
		database,
	)

	db, err := sql.Open("mysql", dsn)

	if err != nil {
		return nil, err
	}

	return &MySqlDriver{
		db: db,
	}, nil
}

func (m *MySqlDriver) CreateMigrationsTable(ctx context.Context) error {
	_, err := m.db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS migrations (
			name VARCHAR(255) PRIMARY KEY NOT NULL,
			executed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)
	`)

	if err != nil {
		return err
	}
	return nil
}

func (m *MySqlDriver) GetExecutedMigrations(ctx context.Context, reverse bool) ([]ExecutedMigration, error) {
	query := `SELECT name, executed_at FROM migrations ORDER BY name ASC`
	if reverse {
		query = `SELECT name, executed_at FROM migrations ORDER BY name DESC`
	}

	rows, err := m.db.QueryContext(ctx, query)

	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var migrations []ExecutedMigration
	for rows.Next() {
		var executedMigrationName string
		var executedMigrationTime time.Time
		if err := rows.Scan(&executedMigrationName, &executedMigrationTime); err != nil {
			return nil, err
		}
		migrations = append(migrations, ExecutedMigration{
			ExecutedAt: executedMigrationTime,
			Name:       executedMigrationName,
		})
	}

	return migrations, nil
}

func (m *MySqlDriver) CleanDatabase(ctx context.Context) error {
	// Step 1: Disable foreign key checks
	_, err := m.db.ExecContext(ctx, `SET FOREIGN_KEY_CHECKS = 0;`)
	if err != nil {
		return err
	}

	// Step 2: Query table names
	rows, err := m.db.QueryContext(ctx, `
		SELECT table_name 
		FROM information_schema.tables 
		WHERE table_schema = DATABASE(); -- use current DB
	`)
	if err != nil {
		return err
	}
	defer rows.Close()

	// Step 3: Build and execute truncate statements
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			return err
		}

		_, err := m.db.ExecContext(ctx, fmt.Sprintf("DROP TABLE `%s`;", table))
		if err != nil {
			return fmt.Errorf("failed to drop table %s: %w", table, err)
		}
	}

	// Step 4: Re-enable foreign key checks
	_, err = m.db.ExecContext(ctx, `SET FOREIGN_KEY_CHECKS = 1;`)
	if err != nil {
		return err
	}

	return nil
}

func (m *MySqlDriver) ApplyMigrations(ctx context.Context, migrations MigrationFiles, onRunning func(migration *MigrationFile), onSuccess func(migration *MigrationFile), onFailed func(migration *MigrationFile, err error)) error {
	return m.transaction(ctx, func(tx *sql.Tx) error {
		for _, migration := range migrations {
			if onRunning != nil {
				onRunning(&migration)
			}
			err := m.executeSqlBytes(ctx, tx, migration.UpSql)
			if err != nil {
				if onFailed != nil {
					onFailed(&migration, err)
				}
				return err
			}
			err = m.insertExecutedMigration(ctx, tx, migration.BaseName, time.Now())
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
	})
}

func (m *MySqlDriver) UnapplyMigrations(ctx context.Context, migrations MigrationFiles, onRunning func(migration *MigrationFile), onSuccess func(migration *MigrationFile), onFailed func(migration *MigrationFile, err error)) error {
	return m.transaction(ctx, func(tx *sql.Tx) error {
		for _, migration := range migrations {
			if onRunning != nil {
				onRunning(&migration)
			}
			err := m.executeSqlBytes(ctx, tx, migration.DownSql)
			if err != nil {
				if onFailed != nil {
					onFailed(&migration, err)
				}
				return err
			}
			err = m.removeExecutedMigration(ctx, tx, migration.BaseName)
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
	})
}

func (m *MySqlDriver) transaction(ctx context.Context, fn func(tx *sql.Tx) error) error {
	log.Println("starting transaction")
	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	err = fn(tx)
	if err != nil {
		log.Println("transaction failed, trying to rollback")
		if rollbackErr := tx.Rollback(); rollbackErr != nil {
			return fmt.Errorf("tx rollback failed: %v, original error: %w", rollbackErr, err)
		}
		return err
	}
	log.Println("transaction succeeded, committing")
	return tx.Commit()
}

func (m *MySqlDriver) executeSqlBytes(ctx context.Context, tx *sql.Tx, sqlBytes []byte, args ...any) error {
	_, err := tx.ExecContext(ctx, string(sqlBytes), args...)

	if err != nil {
		return err
	}
	return nil
}

func (m *MySqlDriver) insertExecutedMigration(ctx context.Context, tx *sql.Tx, name string, executedAt time.Time) error {
	_, err := tx.ExecContext(ctx, `
		INSERT INTO migrations (name, executed_at) VALUES (?, ?)
	`, name, executedAt)

	if err != nil {
		return err
	}
	return nil
}

func (m *MySqlDriver) removeExecutedMigration(ctx context.Context, tx *sql.Tx, name string) error {
	_, err := tx.ExecContext(ctx, `
		DELETE FROM migrations WHERE name = ?
	`, name)

	if err != nil {
		return err
	}
	return nil
}
