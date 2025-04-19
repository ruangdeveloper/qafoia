package qafoia

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"
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
	order := "ASC"
	if reverse {
		order = "DESC"
	}
	query := fmt.Sprintf(`SELECT name, executed_at FROM %s ORDER BY name %s;`, p.migrationTableName, order)

	rows, err := p.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var migrations []ExecutedMigration

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

func (p *PostgresDriver) CleanDatabase(ctx context.Context) error {
	rows, err := p.db.QueryContext(ctx, `
		SELECT tablename
		FROM pg_tables
		WHERE schemaname = 'public';
	`)
	if err != nil {
		return fmt.Errorf("query table names: %w", err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			return fmt.Errorf("scan table name: %w", err)
		}
		tables = append(tables, fmt.Sprintf(`"%s"`, table)) // safely quote identifiers
	}

	if len(tables) == 0 {
		log.Println("no tables to drop")
		return nil
	}

	query := fmt.Sprintf(`DROP TABLE IF EXISTS %s CASCADE;`, strings.Join(tables, ", "))
	if _, err := p.db.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("drop tables: %w", err)
	}

	log.Println("all public tables dropped")
	return nil
}

func (p *PostgresDriver) ApplyMigrations(
	ctx context.Context,
	migrations []Migration,
	onRunning func(migration *Migration),
	onSuccess func(migration *Migration),
	onFailed func(migration *Migration, err error),
) error {
	for i := range migrations {
		m := migrations[i]

		if onRunning != nil {
			onRunning(&m)
		}

		if err := p.executeMigrationSQL(ctx, m.UpScript()); err != nil {
			if onFailed != nil {
				onFailed(&m, err)
			}
			return fmt.Errorf("failed to apply migration %s: %w", m.Name(), err)
		}

		if err := p.insertExecutedMigration(ctx, m.Name(), time.Now()); err != nil {
			if onFailed != nil {
				onFailed(&m, err)
			}
			return fmt.Errorf("failed to record migration %s: %w", m.Name(), err)
		}

		if onSuccess != nil {
			onSuccess(&m)
		}
	}

	return nil
}

func (p *PostgresDriver) UnapplyMigrations(
	ctx context.Context,
	migrations []Migration,
	onRunning func(migration *Migration),
	onSuccess func(migration *Migration),
	onFailed func(migration *Migration, err error),
) error {
	for i := range migrations {
		mig := migrations[i]

		if onRunning != nil {
			onRunning(&mig)
		}

		if err := p.executeMigrationSQL(ctx, mig.DownScript()); err != nil {
			if onFailed != nil {
				onFailed(&mig, err)
			}
			return fmt.Errorf("failed to unapply migration %s: %w", mig.Name(), err)
		}

		if err := p.removeExecutedMigration(ctx, mig.Name()); err != nil {
			if onFailed != nil {
				onFailed(&mig, err)
			}
			return fmt.Errorf("failed to remove migration record %s: %w", mig.Name(), err)
		}

		if onSuccess != nil {
			onSuccess(&mig)
		}
	}

	return nil
}

func (p *PostgresDriver) executeMigrationSQL(ctx context.Context, sql string) error {
	if sql == "" {
		return nil
	}

	_, err := p.db.ExecContext(ctx, sql)

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
