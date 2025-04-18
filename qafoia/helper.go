package qafoia

import (
	"fmt"
	"go/format"
	"os"
	"regexp"
	"sort"
	"strings"

	"golang.org/x/text/cases"
	"golang.org/x/text/language"
)

func fileExists(fileName string) bool {
	_, err := os.Stat(fileName)
	return !os.IsNotExist(err)
}

func migrationDirExists(migrationFilesDir string) bool {
	_, err := os.Stat(migrationFilesDir)
	return !os.IsNotExist(err)
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
	name = strings.ReplaceAll(name, "-", "_")
	name = strings.ReplaceAll(name, " ", "_")
	name = strings.ReplaceAll(name, " ", "_")
	name = strings.ToLower(name)
	name = strings.TrimSpace(name)
	name = strings.Trim(name, "_")
	if len(name) > 200 {
		name = name[:200]
	}

	valid := regexp.MustCompile(`^[a-zA-Z0-9_]+$`)
	if !valid.MatchString(name) {
		return "", fmt.Errorf("invalid migration name: %s", name)
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

func migrationNameToStructName(migrationName string) (string, error) {
	// get the timestamp from prefix
	re := regexp.MustCompile(`^\d{14}_`)
	matches := re.FindStringSubmatch(migrationName)
	if len(matches) == 0 {
		return "", fmt.Errorf("invalid migration name: %s", migrationName)
	}
	timestamp := strings.TrimRight(matches[0], "_")
	// remove the timestamp from the name
	nameWithoutTimestamp := strings.TrimPrefix(migrationName, timestamp)
	fmt.Println(timestamp)
	fmt.Println(nameWithoutTimestamp)

	// split the name by underscore
	parts := strings.Split(nameWithoutTimestamp, "_")
	// capitalize the first letter of each part
	for i, part := range parts {
		parts[i] = cases.Title(language.English).String(part)
	}
	// join the parts together
	structName := fmt.Sprintf("M%s%s", timestamp, strings.Join(parts, ""))

	return structName, nil
}

func getPackageNameFromMigrationDir(migrationFilesDir string) string {
	// get the last part of the migrationFilesDir
	parts := strings.Split(migrationFilesDir, "/")
	if len(parts) == 0 {
		return "migrations"
	}
	return parts[len(parts)-1]
}

func migrationFileTemplate(packageName string, migrationName string) (string, error) {
	structName, err := migrationNameToStructName(migrationName)
	if err != nil {
		return "", err
	}

	migrationTemplate := fmt.Sprintf(`
		package %s

		type %s struct {}

		func (m *%s) Name() string {
			return "%s"
		}

		func (m *%s) UpScript() string {
			return ""
		}

		func (m *%s) DownScript() string {
			return ""
		}
	`,
		packageName,
		structName,
		structName,
		migrationName,
		structName,
		structName,
	)

	formatted, err := format.Source([]byte(migrationTemplate))
	if err != nil {
		return "", err
	}

	return string(formatted), nil
}

func getSortedMigrationName(migrations map[string]Migration) []string {
	keys := []string{}
	for k := range migrations {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	return keys
}
