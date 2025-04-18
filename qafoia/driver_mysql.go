package qafoia

import (
	"context"
	"database/sql"
	"fmt"
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
	query := fmt.Sprintf(`SELECT name, executed_at FROM %s ORDER BY name %s`, m.migrationTableName, ternary(reverse, "DESC", "ASC"))
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
	_, err := m.db.ExecContext(ctx, `SET FOREIGN_KEY_CHECKS = 0;`)
	if err != nil {
		return err
	}

	rows, err := m.db.QueryContext(ctx, `
		SELECT table_name 
		FROM information_schema.tables 
		WHERE table_schema = DATABASE(); -- use current DB
	`)
	if err != nil {
		return err
	}
	defer rows.Close()

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

	_, err = m.db.ExecContext(ctx, `SET FOREIGN_KEY_CHECKS = 1;`)
	if err != nil {
		return err
	}

	return nil
}

func (m *MySqlDriver) ApplyMigrations(ctx context.Context, migrations MigrationFiles, onRunning func(migration *MigrationFile), onSuccess func(migration *MigrationFile), onFailed func(migration *MigrationFile, err error)) error {
	for _, migration := range migrations {
		if onRunning != nil {
			onRunning(&migration)
		}
		err := m.executeMigrationSQL(ctx, migration.UpSql)
		if err != nil {
			if onFailed != nil {
				onFailed(&migration, err)
			}
			return err
		}
		err = m.insertExecutedMigration(ctx, migration.BaseName, time.Now())
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

func (m *MySqlDriver) UnapplyMigrations(ctx context.Context, migrations MigrationFiles, onRunning func(migration *MigrationFile), onSuccess func(migration *MigrationFile), onFailed func(migration *MigrationFile, err error)) error {
	for _, migration := range migrations {
		if onRunning != nil {
			onRunning(&migration)
		}
		err := m.executeMigrationSQL(ctx, migration.DownSql)
		if err != nil {
			if onFailed != nil {
				onFailed(&migration, err)
			}
			return err
		}
		err = m.removeExecutedMigration(ctx, migration.BaseName)
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

func (m *MySqlDriver) executeMigrationSQL(ctx context.Context, sqlBytes []byte, args ...any) error {
	_, err := m.db.ExecContext(ctx, string(sqlBytes), args...)

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
