package qafoia

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/lib/pq"
)

type PostgresDriver struct {
	db                 *sql.DB
	migrationTableName string
}

func NewPostgresDriver(
	host string,
	port string,
	user string,
	password string,
	database string,
	schema string,
) (*PostgresDriver, error) {
	dsn := "host=%s port=%s user=%s password=%s dbname=%s sslmode=disable search_path=%s"
	dsn = fmt.Sprintf(dsn, host, port, user, password, database, schema)

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}

	if err := db.Ping(); err != nil {
		return nil, err
	}

	return &PostgresDriver{
		db:                 db,
		migrationTableName: "migrations",
	}, nil
}

func (p *PostgresDriver) Close() error {
	if p.db != nil {
		if err := p.db.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (p *PostgresDriver) SetMigrationTableName(name string) {
	if name == "" {
		name = "migrations"
	}
	p.migrationTableName = name
}

func (p *PostgresDriver) CreateMigrationsTable(ctx context.Context) error {
	query := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			name VARCHAR(255) PRIMARY KEY NOT NULL,
			executed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		);
	`, p.migrationTableName)

	_, err := p.db.ExecContext(ctx, query)
	if err != nil {
		return err
	}

	return nil
}

func (p *PostgresDriver) GetExecutedMigrations(ctx context.Context, reverse bool) ([]ExecutedMigration, error) {
	query := fmt.Sprintf(`
		SELECT name, executed_at
		FROM %s
		ORDER BY name %s;
	`, p.migrationTableName, ternary(reverse, "DESC", "ASC"))

	rows, err := p.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var migrations []ExecutedMigration

	for rows.Next() {
		var migration ExecutedMigration
		if err := rows.Scan(&migration.Name, &migration.ExecutedAt); err != nil {
			return nil, err
		}
		migrations = append(migrations, migration)
	}

	return migrations, nil
}

func (p *PostgresDriver) CleanDatabase(ctx context.Context) error {
	rows, err := p.db.QueryContext(ctx, `
		SELECT tablename 
		FROM pg_tables 
		WHERE schemaname = 'public';
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

		_, err := p.db.ExecContext(ctx, fmt.Sprintf(`DROP TABLE IF EXISTS "%s" CASCADE;`, table))
		if err != nil {
			return fmt.Errorf("failed to drop table %s: %w", table, err)
		}
	}

	return nil
}

func (p *PostgresDriver) ApplyMigrations(
	ctx context.Context,
	migrations MigrationFiles,
	onRunning func(migration *MigrationFile),
	onSuccess func(migration *MigrationFile),
	onFailed func(migration *MigrationFile, err error),
) error {
	for _, migration := range migrations {
		if onRunning != nil {
			onRunning(&migration)
		}
		err := p.executeMigrationSQL(ctx, migration.UpSql)
		if err != nil {
			if onFailed != nil {
				onFailed(&migration, err)
			}
			return err
		}
		err = p.insertExecutedMigration(ctx, migration.BaseName, time.Now())
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

func (p *PostgresDriver) UnapplyMigrations(
	ctx context.Context,
	migrations MigrationFiles,
	onRunning func(migration *MigrationFile),
	onSuccess func(migration *MigrationFile),
	onFailed func(migration *MigrationFile, err error),
) error {
	for _, migration := range migrations {
		if onRunning != nil {
			onRunning(&migration)
		}
		err := p.executeMigrationSQL(ctx, migration.DownSql)
		if err != nil {
			if onFailed != nil {
				onFailed(&migration, err)
			}
			return err
		}
		err = p.removeExecutedMigration(ctx, migration.BaseName)
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

func (p *PostgresDriver) executeMigrationSQL(ctx context.Context, sqlBytes []byte, args ...any) error {
	_, err := p.db.ExecContext(ctx, string(sqlBytes), args...)

	if err != nil {
		return err
	}
	return nil
}

func (p *PostgresDriver) insertExecutedMigration(ctx context.Context, name string, executedAt time.Time) error {
	query := fmt.Sprintf(`INSERT INTO %s (name, executed_at) VALUES ($1, $2)`, p.migrationTableName)
	_, err := p.db.ExecContext(ctx, query, name, executedAt)

	if err != nil {
		return err
	}
	return nil
}

func (p *PostgresDriver) removeExecutedMigration(ctx context.Context, name string) error {
	query := fmt.Sprintf(`DELETE FROM %s WHERE name = $1`, p.migrationTableName)
	_, err := p.db.ExecContext(ctx, query, name)

	if err != nil {
		return err
	}
	return nil
}
