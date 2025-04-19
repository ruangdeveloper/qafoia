# 🧠 Qafoia

**Qafoia** is a lightweight and extensible database migration library for Go, designed with simplicity and flexibility in mind. It allows you to register migrations in Go code, manage them efficiently, and execute or roll them back in a safe and structured manner.

## ✨ Features

- Register migrations using Go structs.
- Run, rollback, reset, or re-run migrations with ease.
- Debug SQL output during migration execution.
- Automatically generates migration file templates.
- Tracks applied migrations using a dedicated database table.
- Ready-to-use MySQL and Postgres drivers.

## 📦 Installation

To install Qafoia, use the following command:

```bash
go get github.com/ruangdeveloper/qafoia
```

## 🛠️ Usage

### 1. Initialize Qafoia

First, configure the library by providing a `Config`:

```go
cfg := &qafoia.Config{
    Driver:             yourDriver,  // Must implement qafoia.Driver
    MigrationFilesDir:  "migrations",  // Optional: default is "migrations"
    MigrationTableName: "migrations",  // Optional: default is "migrations"
    DebugSql:           true,  // Optional: enables SQL debugging
}

q, err := qafoia.New(cfg)
if err != nil {
    log.Fatal(err)
}
```

### 2. Register Migrations

```go
q.Register(
    migration1, // Must implement qafoia.Migration
    migration2,
)
```
Migration struct is created automatically when creating migration file.

### 3. Apply Migrations

To apply the migrations:

```go
err := q.Migrate(context.Background())
if err != nil {
    log.Fatal(err)
}
```

### 4. Other Operations

- **Create a new migration file:**

  ```go
  q.Create("add_users_table")
  ```

- **Run fresh migrations (clean + migrate):**

  ```go
  q.Fresh(context.Background())
  ```

- **Reset migrations (rollback all and reapply):**

  ```go
  q.Reset(context.Background())
  ```

- **Rollback last `n` migrations:**

  ```go
  q.Rollback(context.Background(), 2)
  ```

- **Clean the database:**

  ```go
  q.Clean(context.Background())
  ```

- **List all registered migrations and their status:**

  ```go
  list, err := q.List(context.Background())
  ```

## 📁 Migration Interface

Each migration must implement the following interface:

```go
type Migration interface {
    Name() string
    UpScript() string
    DownScript() string
}
```

## 🔌 Driver Interface

You can use any database driver that implements the `Driver` interface. We currently provide ready-to-use MySQL and Postgres drivers.

### MySQL Driver

To use the MySQL driver:

```go
d, err := qafoia.NewMySqlDriver(
    "localhost", // Host
    "3306",      // Port
    "root",      // Username
    "",          // Password
    "qafoia",    // Database Name
    "utf8mb4",   // Charset
)
```

### Postgres Driver

To use the Postgres driver:

```go
d, err := qafoia.NewPostgresDriver(
    "localhost", // Host
    "5432",      // Port
    "root",      // Username
    "",          // Password
    "qafoia",    // Database Name
    "public",    // Schema
)
```

## 📦 Generated Migration File Example

When you run `q.Create("create_users_table")`, a file like this will be created:

```go
package migrations

type M20250418220011CreateUsersTable struct{}

func (m *M20250418220011CreateUsersTable) Name() string {
	return "20250418220011_create_users_table"
}

func (m *M20250418220011CreateUsersTable) UpScript() string {
	return `
        -- write sql script here
    `
}

func (m *M20250418220011CreateUsersTable) DownScript() string {
	return `
        -- write sql script for revert the changes here
    `
}

```

## 🧑‍💻 CLI Helper

Qafoia provides an optional CLI helper that simplifies running migrations and other tasks. You can use the CLI as follows:

### 1. Initialize the CLI

You can initialize the CLI with the `CliConfig`:

```go
cli, err := qafoia.NewCli(qafoia.CliConfig{
    Qafoia: q,  // The Qafoia instance you've initialized earlier
})

if err != nil {
    log.Fatal(err)
}

err = cli.Execute(context.TODO())

if err != nil {
    log.Fatal(err)
}
```

### 2. Run CLI Commands

After setting up the CLI, you can run the following commands directly from the terminal:

- **Clean database (delete all tables):**

  ```bash
  go run main.go clean
  ```

- **Create a new migration:**

  ```bash
  go run main.go create
  ```

- **List all migrations:**

  ```bash
  go run main.go list
  ```

- **Run all pending migrations:**

  ```bash
  go run main.go migrate
  ```

- **Rollback all migrations and re-run all migrations:**

  ```bash
  go run main.go reset
  ```

- **Rollback the last migration:**

  ```bash
  go run main.go rollback
  ```

These commands are built into the CLI, making it easy to perform common migration tasks without having to write custom code each time.

### Full Example
```go
package main

import (
	"context"
	"log"

	"github.com/ruangdeveloper/qafoia"
	"your_app/migrations"
)

func main() {
	d, err := qafoia.NewMySqlDriver(
		"localhost",
		"3306",
		"root",
		"",
		"qafoia",
		"utf8mb4",
	)

	if err != nil {
		log.Fatal(err)
	}
	q, err := qafoia.New(&qafoia.Config{
		Driver:             d,
		MigrationFilesDir:  "migrations",
		MigrationTableName: "migrations",
		DebugSql:           false,
	})

	if err != nil {
		log.Fatal(err)
	}

	err = q.Register(
		&mysql.M20250418220011CreateUsersTable{},
		&mysql.M20250418233018CreateRolesTable{},
	)

	if err != nil {
		log.Fatal(err)
	}

	cli, err := qafoia.NewCli(qafoia.CliConfg{
		Qafoia: q,
	})

	if err != nil {
		log.Fatal(err)
	}

	err = cli.Execute(context.TODO())

	if err != nil {
		log.Fatal(err)
	}
}
```

## 📄 License

MIT License

---

Made with ❤️ by [Ruang Developer](https://github.com/ruangdeveloper)