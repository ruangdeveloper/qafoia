package qafoia

import (
	"fmt"
	"time"
)

type MigrationFile struct {
	BaseName   string `json:"base_name"`
	UpName     string `json:"up_name"`
	UpSql      []byte `json:"up_sql"`
	DownName   string `json:"down_name"`
	DownSql    []byte `json:"down_sql"`
	IsExecuted bool   `json:"is_executed"`
}

type MigrationFiles []MigrationFile

func (m MigrationFiles) Print() {
	var tableData [][]string
	tableData = append(tableData, []string{"Base Name", "Is Executed"})

	for _, migration := range m {
		row := []string{
			migration.BaseName,
			fmt.Sprintf("%t", migration.IsExecuted),
		}
		tableData = append(tableData, row)
	}

	printTable(tableData)
}

type ExecutedMigration struct {
	Name       string    `json:"name"`
	ExecutedAt time.Time `json:"executed_at"`
}

type Config struct {
	Driver             Driver
	MigrationFilesDir  string
	MigrationTableName string
	DebugSql           bool
}
