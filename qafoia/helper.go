package qafoia

import (
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"
	"sync"
)

func findMigrationFile(baseName string, migrationFiles []MigrationFile) (*MigrationFile, bool) {
	for _, migrationFile := range migrationFiles {
		if migrationFile.BaseName == baseName {
			return &migrationFile, true
		}
	}
	return nil, false
}

func findExecutedMigration(baseName string, executedMigrations []ExecutedMigration) (*ExecutedMigration, bool) {
	for _, executedMigration := range executedMigrations {
		if executedMigration.Name == baseName {
			return &executedMigration, true
		}
	}
	return nil, false
}

func fileExists(fileName string) bool {
	_, err := os.Stat(fileName)
	return !os.IsNotExist(err)
}

func migrationDirExists(migrationFilesDir string) bool {
	_, err := os.Stat(migrationFilesDir)
	return !os.IsNotExist(err)
}

func collectMigrationFiles(
	migrationFilesDir string,
) ([]MigrationFile, error) {
	files, err := os.ReadDir(migrationFilesDir)
	if err != nil {
		return nil, err
	}

	upChan := make(chan struct {
		baseName string
		fileName string
		sql      []byte
	})
	downChan := make(chan struct {
		baseName string
		fileName string
		sql      []byte
	})
	errChan := make(chan error, 1)

	var wg sync.WaitGroup

	for _, file := range files {
		if file.IsDir() {
			continue
		}
		wg.Add(1)
		go func(file os.DirEntry) {
			defer wg.Done()
			fileName := file.Name()
			switch {
			case len(fileName) > 7 && fileName[len(fileName)-7:] == ".up.sql":
				base := fileName[:len(fileName)-7]
				upFilePath := fmt.Sprintf("%s/%s", migrationFilesDir, fileName)
				upSql, err := os.ReadFile(upFilePath)
				if err != nil {
					select {
					case errChan <- fmt.Errorf("failed to read up file %s: %w", fileName, err):
					default:
					}
					return
				}
				upChan <- struct {
					baseName string
					fileName string
					sql      []byte
				}{base, fileName, upSql}
			case len(fileName) > 9 && fileName[len(fileName)-9:] == ".down.sql":
				base := fileName[:len(fileName)-9]
				downFilePath := fmt.Sprintf("%s/%s", migrationFilesDir, fileName)
				downSql, err := os.ReadFile(downFilePath)
				if err != nil {
					select {
					case errChan <- fmt.Errorf("failed to read down file %s: %w", fileName, err):
					default:
					}
					return
				}
				downChan <- struct {
					baseName string
					fileName string
					sql      []byte
				}{base, fileName, downSql}
			default:
				select {
				case errChan <- fmt.Errorf("invalid migration file name %s: must end with .up.sql or .down.sql", fileName):
				default:
				}
				return
			}
		}(file)
	}

	go func() {
		wg.Wait()
		close(upChan)
		close(downChan)
	}()

	upNames := make(map[string]string)
	upSqls := make(map[string][]byte)
	downNames := make(map[string]string)
	downSqls := make(map[string][]byte)

	var innerWg sync.WaitGroup
	innerWg.Add(2)

	go func() {
		defer innerWg.Done()
		for up := range upChan {
			upNames[up.baseName] = up.fileName
			upSqls[up.baseName] = up.sql
		}
	}()

	go func() {
		defer innerWg.Done()
		for down := range downChan {
			downNames[down.baseName] = down.fileName
			downSqls[down.baseName] = down.sql
		}
	}()

	innerWg.Wait()

	select {
	case err := <-errChan:
		return nil, err
	default:
	}

	done := make(chan struct{})

	go func() {
		for up := range upChan {
			upNames[up.baseName] = up.fileName
		}
	}()

	go func() {
		for down := range downChan {
			downNames[down.baseName] = down.fileName
		}
		close(done)
	}()

	<-done

	var migrationFiles []MigrationFile
	for base, up := range upNames {
		if down, ok := downNames[base]; ok {
			migrationFiles = append(migrationFiles, MigrationFile{
				BaseName: base,
				UpName:   up,
				DownName: down,
				UpSql:    upSqls[base],
				DownSql:  downSqls[base],
			})
		}
	}

	sort.Slice(migrationFiles, func(i, j int) bool {
		return migrationFiles[i].BaseName < migrationFiles[j].BaseName
	})

	return migrationFiles, nil
}

func printTable(data [][]string) {
	if len(data) == 0 {
		fmt.Println("No data to display.")
		return
	}

	colWidths := make([]int, len(data[0]))
	for _, row := range data {
		for colIdx, col := range row {
			if len(col) > colWidths[colIdx] {
				colWidths[colIdx] = len(col)
			}
		}
	}

	printRow := func(row []string) {
		fmt.Print("|")
		for i, col := range row {
			format := fmt.Sprintf(" %%-%ds |", colWidths[i])
			fmt.Printf(format, col)
		}
		fmt.Println()
	}

	printSeparator := func() {
		fmt.Print("+")
		for _, width := range colWidths {
			fmt.Print(strings.Repeat("-", width+2) + "+")
		}
		fmt.Println()
	}

	printSeparator()
	printRow(data[0])
	printSeparator()

	for _, row := range data[1:] {
		printRow(row)
	}
	printSeparator()

}

func sanitizeMigrationName(name string) (string, error) {
	valid := regexp.MustCompile(`^[a-zA-Z0-9_]+$`)
	if !valid.MatchString(name) {
		return "", fmt.Errorf("invalid migration name: %s", name)
	}
	name = strings.ToLower(name)
	name = strings.TrimSpace(name)
	name = strings.Trim(name, "_")
	if len(name) > 200 {
		name = name[:200]
	}
	return name, nil
}

func sanitizeTableName(name string) (string, error) {
	valid := regexp.MustCompile(`^[a-zA-Z0-9_]+$`)
	if !valid.MatchString(name) {
		return "", fmt.Errorf("invalid table name: %s", name)
	}

	return name, nil
}

func ternary[T any](condition bool, a, b T) T {
	if condition {
		return a
	}
	return b
}
