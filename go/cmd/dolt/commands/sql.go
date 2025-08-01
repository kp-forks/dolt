// Copyright 2019-2020 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package commands

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/abiosoft/readline"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/ishell"
	"github.com/dolthub/vitess/go/vt/sqlparser"
	"github.com/dolthub/vitess/go/vt/vterrors"
	"github.com/fatih/color"
	"github.com/flynn-archive/go-shlex"
	"github.com/gocraft/dbr/v2"
	"github.com/gocraft/dbr/v2/dialect"
	textunicode "golang.org/x/text/encoding/unicode"
	"golang.org/x/text/transform"
	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/commands/engine"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	dsqle "github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/libraries/utils/iohelp"
	"github.com/dolthub/dolt/go/libraries/utils/osutil"
	"github.com/dolthub/dolt/go/store/val"
	eventsapi "github.com/dolthub/eventsapi_schema/dolt/services/eventsapi/v1alpha1"
)

var sqlDocs = cli.CommandDocumentationContent{
	ShortDesc: "Runs a SQL query",
	LongDesc: `Runs a SQL query you specify. With no arguments, begins an interactive shell to run queries and view the results. With the {{.EmphasisLeft}}-q{{.EmphasisRight}} option, runs the given query and prints any results, then exits.

Multiple SQL statements must be separated by semicolons. Use {{.EmphasisLeft}}-b{{.EmphasisRight}} to enable batch mode to speed up large batches of INSERT / UPDATE statements. Pipe SQL files to dolt sql (no {{.EmphasisLeft}}-q{{.EmphasisRight}}) to execute a SQL import or update script. 

By default this command uses the dolt database in the current working directory. If you would prefer to use a different directory, user the {{.EmphasisLeft}}--data-dir <directory>{{.EmphasisRight}} argument before the sql subcommand.

If a server is running for the database in question, then the query will go through the server automatically. If connecting to a remote server is preferred, used the {{.EmphasisLeft}}--host <host>{{.EmphasisRight}} and {{.EmphasisLeft}}--port <port>{{.EmphasisRight}} global arguments. See 'dolt --help' for more information about global arguments.`,

	Synopsis: []string{
		"",
		"< script.sql",
		"-q {{.LessThan}}query{{.GreaterThan}} [-r {{.LessThan}}result format{{.GreaterThan}}] [-s {{.LessThan}}name{{.GreaterThan}} -m {{.LessThan}}message{{.GreaterThan}}] [-b]",
		"-x {{.LessThan}}name{{.GreaterThan}}",
		"--list-saved",
	},
}

var ErrMultipleDoltCfgDirs = errors.NewKind("multiple .doltcfg directories detected: '%s' and '%s'; pass one of the directories using option --doltcfg-dir")

const (
	QueryFlag             = "query"
	FormatFlag            = "result-format"
	saveFlag              = "save"
	executeFlag           = "execute"
	listSavedFlag         = "list-saved"
	messageFlag           = "message"
	BatchFlag             = "batch"
	DataDirFlag           = "data-dir"
	MultiDBDirFlag        = "multi-db-dir"
	CfgDirFlag            = "doltcfg-dir"
	DefaultCfgDirName     = ".doltcfg"
	PrivsFilePathFlag     = "privilege-file"
	BranchCtrlPathFlag    = "branch-control-file"
	DefaultPrivsName      = "privileges.db"
	DefaultBranchCtrlName = "branch_control.db"
	continueFlag          = "continue"
	fileInputFlag         = "file"
	UserFlag              = "user"
	DefaultUser           = "root"
	DefaultHost           = "localhost"
	UseDbFlag             = "use-db"
	ProfileFlag           = "profile"
	timeFlag              = "time"
	outputFlag            = "output"
	binaryAsHexFlag       = "binary-as-hex"
	skipBinaryAsHexFlag   = "skip-binary-as-hex"
	// TODO: Consider simplifying to use MySQL's skip pattern with single flag definition
	// MySQL handles both --binary-as-hex and --skip-binary-as-hex with one option definition
	// and uses disabled_my_option to distinguish between enable/disable

	welcomeMsg = `# Welcome to the DoltSQL shell.
# Statements must be terminated with ';'.
# "exit" or "quit" (or Ctrl-D) to exit. "\help" for help.`
)

// TODO: get rid of me, use a real integration point to define system variables
func init() {
	dsqle.AddDoltSystemVariables()
}

type SqlCmd struct {
	VersionStr string
}

// The SQL shell installs its own signal handlers so that you can cancel a running query without ending the entire
// process
func (cmd SqlCmd) InstallsSignalHandlers() bool {
	return true
}

var _ cli.SignalCommand = SqlCmd{}

// Name returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd SqlCmd) Name() string {
	return "sql"
}

// Description returns a description of the command
func (cmd SqlCmd) Description() string {
	return "Run a SQL query against tables in repository."
}

func (cmd SqlCmd) Docs() *cli.CommandDocumentation {
	ap := cmd.ArgParser()
	return cli.NewCommandDocumentation(sqlDocs, ap)
}

func (cmd SqlCmd) ArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParserWithMaxArgs(cmd.Name(), 0)
	ap.SupportsString(QueryFlag, "q", "SQL query to run", "Runs a single query and exits.")
	ap.SupportsString(FormatFlag, "r", "result output format", "How to format result output. Valid values are tabular, csv, json, vertical, and parquet. Defaults to tabular.")
	ap.SupportsString(saveFlag, "s", "saved query name", "Used with --query, save the query to the query catalog with the name provided. Saved queries can be examined in the dolt_query_catalog system table.")
	ap.SupportsString(executeFlag, "x", "saved query name", "Executes a saved query with the given name.")
	ap.SupportsFlag(listSavedFlag, "l", "List all saved queries.")
	ap.SupportsString(messageFlag, "m", "saved query description", "Used with --query and --save, saves the query with the descriptive message given. See also `--name`.")
	ap.SupportsFlag(BatchFlag, "b", "Use to enable more efficient batch processing for large SQL import scripts. This mode is no longer supported and this flag is a no-op. To speed up your SQL imports, use either LOAD DATA, or structure your SQL import script to insert many rows per statement.")
	ap.SupportsFlag(continueFlag, "c", "Continue running queries on an error. Used for batch mode only.")
	ap.SupportsString(fileInputFlag, "f", "input file", "Execute statements from the file given.")
	ap.SupportsFlag(binaryAsHexFlag, "", "Print binary data as hex. Enabled by default for interactive terminals.")
	// TODO: MySQL uses a skip- pattern for negating flags and doesn't show them in help
	ap.SupportsFlag(skipBinaryAsHexFlag, "", "Disable binary data as hex output.")
	return ap
}

// EventType returns the type of the event to log
func (cmd SqlCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_SQL
}

// RequiresRepo indicates that this command does not have to be run from within a dolt data repository directory.
// In this case it is because this command supports the DataDirFlag which can pass in a directory.  In the event that
// that parameter is not provided there is additional error handling within this command to make sure that this was in
// fact run from within a dolt data repository directory.
func (cmd SqlCmd) RequiresRepo() bool {
	return false
}

// Exec executes the command
// Unlike other commands, sql doesn't set a new working root directly, as the SQL layer updates the working set as
// necessary when committing work.
func (cmd SqlCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv, cliCtx cli.CliContext) int {
	ap := cmd.ArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, sqlDocs, ap))
	apr, err := cmd.handleLegacyArguments(ap, commandStr, args)
	if err != nil {
		if err == argparser.ErrHelp {
			help()
			return 0
		}
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}

	err = validateSqlArgs(apr)
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}

	globalArgs := cliCtx.GlobalArgs()
	err = validateSqlArgs(globalArgs)
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}

	// We need a username and password for many SQL commands, so set defaults if they don't exist
	dEnv.Config.SetFailsafes(env.DefaultFailsafeConfig)

	format := engine.FormatTabular
	if formatSr, ok := apr.GetValue(FormatFlag); ok {
		var verr errhand.VerboseError
		format, verr = GetResultFormat(formatSr)
		if verr != nil {
			return HandleVErrAndExitCode(verr, usage)
		}
	}

	// restrict LOAD FILE invocations to current directory
	wd, err := os.Getwd()
	if err != nil {
		wd = "/dev/null"
	}
	err = sql.SystemVariables.AssignValues(map[string]interface{}{
		"secure_file_priv": wd,
	})
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}

	// Determine binary-as-hex behavior from flags (default false for non-interactive modes)
	binaryAsHex := apr.Contains(binaryAsHexFlag)
	if binaryAsHex && apr.Contains(skipBinaryAsHexFlag) { // We stray from MYSQL here to make usage clear for users
		return HandleVErrAndExitCode(errhand.BuildDError("cannot use both --%s and --%s", binaryAsHexFlag, skipBinaryAsHexFlag).Build(), usage)
	}

	queryist, sqlCtx, closeFunc, err := cliCtx.QueryEngine(ctx)
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}
	if closeFunc != nil {
		defer closeFunc()
	}

	if query, queryOK := apr.GetValue(QueryFlag); queryOK {
		if apr.Contains(saveFlag) {
			return SaveQuery(sqlCtx, queryist, apr, query, format, usage, binaryAsHex)
		}
		return queryMode(sqlCtx, queryist, apr, query, format, usage, binaryAsHex)
	} else if savedQueryName, exOk := apr.GetValue(executeFlag); exOk {
		return executeSavedQuery(sqlCtx, queryist, savedQueryName, format, usage, binaryAsHex)
	} else if apr.Contains(listSavedFlag) {
		return listSavedQueries(sqlCtx, queryist, format, usage)
	} else {
		// Run in either batch mode for piped input, or shell mode for interactive
		isTty := false
		fi, err := os.Stdin.Stat()
		if err != nil {
			if !osutil.IsWindows {
				return sqlHandleVErrAndExitCode(queryist, errhand.BuildDError("Couldn't stat STDIN. This is a bug.").Build(), usage)
			}
		} else {
			isTty = fi.Mode()&os.ModeCharDevice != 0
		}

		_, continueOnError := apr.GetValue(continueFlag)

		var input io.Reader = os.Stdin
		if fileInput, ok := apr.GetValue(fileInputFlag); ok {
			isTty = false
			input, err = os.OpenFile(fileInput, os.O_RDONLY, os.ModePerm)
			if err != nil {
				return sqlHandleVErrAndExitCode(queryist, errhand.BuildDError("couldn't open file %s", fileInput).Build(), usage)
			}
			info, err := os.Stat(fileInput)
			if err != nil {
				return sqlHandleVErrAndExitCode(queryist, errhand.BuildDError("couldn't get file size %s", fileInput).Build(), usage)
			}

			input = transform.NewReader(input, textunicode.BOMOverride(transform.Nop))

			// initialize fileReadProg global variable if there is a file to process queries from
			fileReadProg = &fileReadProgress{bytesRead: 0, totalBytes: info.Size(), printed: 0, displayStrLen: 0}
			defer fileReadProg.close()
		}

		if isTty {
			// In shell mode, default to hex format unless explicitly disabled
			shellBinaryAsHex := !apr.Contains(skipBinaryAsHexFlag)
			err := execShell(sqlCtx, queryist, format, cliCtx, shellBinaryAsHex)
			if err != nil {
				return sqlHandleVErrAndExitCode(queryist, errhand.VerboseErrorFromError(err), usage)
			}
		} else {
			input = transform.NewReader(input, textunicode.BOMOverride(transform.Nop))
			err := execBatchMode(sqlCtx, queryist, input, continueOnError, format, binaryAsHex)
			if err != nil {
				return sqlHandleVErrAndExitCode(queryist, errhand.VerboseErrorFromError(err), usage)
			}
		}
	}

	return 0
}

// sqlHandleVErrAndExitCode is a helper function to print errors to the user. Currently, the Queryist interface is used to
// determine if this is a local or remote execution. This is hacky, and too simplistic. We should possibly add an error
// messaging interface to the CliContext.
func sqlHandleVErrAndExitCode(queryist cli.Queryist, verr errhand.VerboseError, usage cli.UsagePrinter) int {
	if verr != nil {
		if msg := verr.Verbose(); strings.TrimSpace(msg) != "" {
			cli.PrintErrln(msg)
		}

		if verr.ShouldPrintUsage() {
			usage()
		}

		return 1
	}

	return 0
}

// handleLegacyArguments is a temporary function to parse args, and print a error and explanation when the old form is provided.
func (cmd SqlCmd) handleLegacyArguments(ap *argparser.ArgParser, commandStr string, args []string) (*argparser.ArgParseResults, error) {

	apr, err := ap.Parse(args)

	if err != nil {
		legacyParser := argparser.NewArgParserWithMaxArgs(cmd.Name(), 0)
		legacyParser.SupportsString(QueryFlag, "q", "SQL query to run", "Runs a single query and exits.")
		legacyParser.SupportsString(FormatFlag, "r", "result output format", "How to format result output. Valid values are tabular, csv, json, vertical, and parquet. Defaults to tabular.")
		legacyParser.SupportsString(saveFlag, "s", "saved query name", "Used with --query, save the query to the query catalog with the name provided. Saved queries can be examined in the dolt_query_catalog system table.")
		legacyParser.SupportsString(executeFlag, "x", "saved query name", "Executes a saved query with the given name.")
		legacyParser.SupportsFlag(listSavedFlag, "l", "List all saved queries.")
		legacyParser.SupportsString(messageFlag, "m", "saved query description", "Used with --query and --save, saves the query with the descriptive message given. See also `--name`.")
		legacyParser.SupportsFlag(BatchFlag, "b", "Use to enable more efficient batch processing for large SQL import scripts. This mode is no longer supported and this flag is a no-op. To speed up your SQL imports, use either LOAD DATA, or structure your SQL import script to insert many rows per statement.")
		legacyParser.SupportsString(DataDirFlag, "", "directory", "Defines a directory whose subdirectories should all be dolt data repositories accessible as independent databases within. Defaults to the current directory.")
		legacyParser.SupportsString(MultiDBDirFlag, "", "directory", "Defines a directory whose subdirectories should all be dolt data repositories accessible as independent databases within. Defaults to the current directory. This is deprecated, you should use `--data-dir` instead")
		legacyParser.SupportsString(CfgDirFlag, "", "directory", "Defines a directory that contains configuration files for dolt. Defaults to `$data-dir/.doltcfg`. Will only be created if there is a change that affect configuration settings.")
		legacyParser.SupportsFlag(continueFlag, "c", "Continue running queries on an error. Used for batch mode only.")
		legacyParser.SupportsString(fileInputFlag, "f", "input file", "Execute statements from the file given.")
		legacyParser.SupportsString(PrivsFilePathFlag, "", "privilege file", "Path to a file to load and store users and grants. Defaults to `$doltcfg-dir/privileges.db`. Will only be created if there is a change to privileges.")
		legacyParser.SupportsString(BranchCtrlPathFlag, "", "branch control file", "Path to a file to load and store branch control permissions. Defaults to `$doltcfg-dir/branch_control.db`. Will only be created if there is a change to branch control permissions.")
		legacyParser.SupportsString(UserFlag, "u", "user", fmt.Sprintf("Defines the local superuser (defaults to `%v`). If the specified user exists, will take on permissions of that user.", DefaultUser))

		_, newErr := legacyParser.Parse(args)

		if newErr != nil {
			// Neither form of the arguments works. Print the usage and the error of the first parse.
			return nil, err
		}

		// The legacy form worked, so print an error and exit.
		err = fmt.Errorf("SQL arguments have changed. Move global arguments before the sql sub command.")
		return nil, err
	}

	return apr, nil

}

func listSavedQueries(ctx *sql.Context, qryist cli.Queryist, format engine.PrintResultFormat, usage cli.UsagePrinter) int {
	query := "SELECT * FROM " + doltdb.DoltQueryCatalogTableName
	return sqlHandleVErrAndExitCode(qryist, execSingleQuery(ctx, qryist, query, format, false), usage)
}

func executeSavedQuery(ctx *sql.Context, qryist cli.Queryist, savedQueryName string, format engine.PrintResultFormat, usage cli.UsagePrinter, binaryAsHex bool) int {
	var buffer bytes.Buffer
	buffer.WriteString("SELECT query FROM dolt_query_catalog where id = ?")
	searchQuery, err := dbr.InterpolateForDialect(buffer.String(), []interface{}{savedQueryName}, dialect.MySQL)

	rows, err := GetRowsForSql(qryist, ctx, searchQuery)
	if err != nil {
		return sqlHandleVErrAndExitCode(qryist, errhand.VerboseErrorFromError(err), usage)
	} else if len(rows) == 0 {
		err = fmt.Errorf("saved query %s not found", savedQueryName)
		return sqlHandleVErrAndExitCode(qryist, errhand.VerboseErrorFromError(err), usage)
	}

	var query string
	if ts, ok := rows[0][0].(*val.TextStorage); ok {
		query, err = ts.Unwrap(ctx)
		if err != nil {
			return sqlHandleVErrAndExitCode(qryist, errhand.VerboseErrorFromError(err), usage)
		}
	} else {
		if s, ok := rows[0][0].(string); ok {
			query = s
		}
	}

	cli.PrintErrf("Executing saved query '%s':\n%s\n", savedQueryName, query)
	return sqlHandleVErrAndExitCode(qryist, execSingleQuery(ctx, qryist, query, format, binaryAsHex), usage)
}

func queryMode(
	ctx *sql.Context,
	qryist cli.Queryist,
	apr *argparser.ArgParseResults,
	query string,
	format engine.PrintResultFormat,
	usage cli.UsagePrinter,
	binaryAsHex bool,
) int {
	_, continueOnError := apr.GetValue(continueFlag)

	input := strings.NewReader(query)
	err := execBatchMode(ctx, qryist, input, continueOnError, format, binaryAsHex)
	if err != nil {
		return sqlHandleVErrAndExitCode(qryist, errhand.VerboseErrorFromError(err), usage)
	}

	return 0
}

func SaveQuery(ctx *sql.Context, qryist cli.Queryist, apr *argparser.ArgParseResults, query string, format engine.PrintResultFormat, usage cli.UsagePrinter, binaryAsHex bool) int {
	saveName := apr.GetValueOrDefault(saveFlag, "")

	verr := execSingleQuery(ctx, qryist, query, format, binaryAsHex)
	if verr != nil {
		return sqlHandleVErrAndExitCode(qryist, verr, usage)
	}

	order := int32(1)
	rows, err := GetRowsForSql(qryist, ctx, "SELECT MAX(display_order) FROM dolt_query_catalog")
	if err != nil {
		return sqlHandleVErrAndExitCode(qryist, errhand.VerboseErrorFromError(err), usage)
	}
	if len(rows) > 0 && rows[0][0] != nil {
		order, err = getInt32ColAsInt32(rows[0][0])
		if err != nil {
			return sqlHandleVErrAndExitCode(qryist, errhand.VerboseErrorFromError(err), usage)
		}
		order++
	}

	saveMessage := apr.GetValueOrDefault(messageFlag, "")
	var buffer bytes.Buffer
	buffer.WriteString("INSERT INTO dolt_query_catalog VALUES (?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE query = ?, description = ?")
	params := []interface{}{saveName, order, saveName, query, saveMessage, query, saveMessage}
	insertQuery, err := dbr.InterpolateForDialect(buffer.String(), params, dialect.MySQL)
	if err != nil {
		return sqlHandleVErrAndExitCode(qryist, errhand.VerboseErrorFromError(err), usage)
	}

	_, err = GetRowsForSql(qryist, ctx, insertQuery)

	return sqlHandleVErrAndExitCode(qryist, errhand.VerboseErrorFromError(err), usage)
}

// execSingleQuery runs a single query and prints the results. This is not intended for use in interactive modes, especially
// the shell.
func execSingleQuery(
	sqlCtx *sql.Context,
	qryist cli.Queryist,
	query string,
	format engine.PrintResultFormat,
	binaryAsHex bool,
) errhand.VerboseError {

	sqlSch, rowIter, _, err := processQuery(sqlCtx, query, qryist)
	if err != nil {
		return formatQueryError("", err)
	}

	if rowIter != nil {
		err = engine.PrettyPrintResults(sqlCtx, format, sqlSch, rowIter, false, false, false, binaryAsHex)
		if err != nil {
			return errhand.VerboseErrorFromError(err)
		}
	}

	return nil
}

func formatQueryError(message string, err error) errhand.VerboseError {
	const (
		maxStatementLen     = 128
		maxPosWhenTruncated = 64
	)

	if se, ok := vterrors.AsSyntaxError(err); ok {
		verrBuilder := errhand.BuildDError("%s", message)
		verrBuilder.AddDetails("Error parsing SQL: ")
		verrBuilder.AddDetails("%s", se.Message)

		statement := se.Statement
		position := se.Position

		prevLines := ""
		for {
			idxNewline := strings.IndexRune(statement, '\n')

			if idxNewline == -1 {
				break
			} else if idxNewline < position {
				position -= idxNewline + 1
				prevLines += statement[:idxNewline+1]
				statement = statement[idxNewline+1:]
			} else {
				statement = statement[:idxNewline]
				break
			}
		}

		if len(statement) > maxStatementLen {
			if position > maxPosWhenTruncated {
				statement = statement[position-maxPosWhenTruncated:]
				position = maxPosWhenTruncated
			}

			if len(statement) > maxStatementLen {
				statement = statement[:maxStatementLen]
			}
		}

		verrBuilder.AddDetails("%s%s", prevLines, statement)

		marker := make([]rune, position+1)
		for i := 0; i < position; i++ {
			marker[i] = ' '
		}

		marker[position] = '^'
		verrBuilder.AddDetails("%s", string(marker))

		return verrBuilder.Build()
	} else {
		if len(message) > 0 {
			err = fmt.Errorf("%s: %v", message, err)
		}
		return errhand.VerboseErrorFromError(err)
	}
}

func GetResultFormat(format string) (engine.PrintResultFormat, errhand.VerboseError) {
	switch strings.ToLower(format) {
	case "tabular":
		return engine.FormatTabular, nil
	case "csv":
		return engine.FormatCsv, nil
	case "json":
		return engine.FormatJson, nil
	case "null":
		return engine.FormatNull, nil
	case "vertical":
		return engine.FormatVertical, nil
	case "parquet":
		return engine.FormatParquet, nil
	default:
		return engine.FormatTabular, errhand.BuildDError("Invalid argument for --result-format. Valid values are tabular, csv, json").Build()
	}
}

func validateSqlArgs(apr *argparser.ArgParseResults) error {
	_, query := apr.GetValue(QueryFlag)
	_, save := apr.GetValue(saveFlag)
	_, msg := apr.GetValue(messageFlag)
	_, list := apr.GetValue(listSavedFlag)
	_, execute := apr.GetValue(executeFlag)
	_, dataDir := apr.GetValue(DataDirFlag)
	_, multiDbDir := apr.GetValue(MultiDBDirFlag)

	if len(apr.Args) > 0 && !query {
		return errhand.BuildDError("Invalid Argument: use --query or -q to pass inline SQL queries").Build()
	}

	if dataDir && multiDbDir {
		return errhand.BuildDError("Invalid Argument: --data-dir is not compatible with --multi-db-dir").Build()
	}

	if execute {
		if list {
			return errhand.BuildDError("Invalid Argument: --execute|-x is not compatible with --list-saved").Build()
		} else if query {
			return errhand.BuildDError("Invalid Argument: --execute|-x is not compatible with --query|-q").Build()
		} else if msg {
			return errhand.BuildDError("Invalid Argument: --execute|-x is not compatible with --message|-m").Build()
		} else if save {
			return errhand.BuildDError("Invalid Argument: --execute|-x is not compatible with --save|-s").Build()
		} else if dataDir || multiDbDir {
			return errhand.BuildDError("Invalid Argument: --execute|-x is not compatible with --data-dir").Build()
		}
	}

	if list {
		if execute {
			return errhand.BuildDError("Invalid Argument: --list-saved is not compatible with --executed|x").Build()
		} else if query {
			return errhand.BuildDError("Invalid Argument: --list-saved is not compatible with --query|-q").Build()
		} else if msg {
			return errhand.BuildDError("Invalid Argument: --list-saved is not compatible with --message|-m").Build()
		} else if save {
			return errhand.BuildDError("Invalid Argument: --list-saved is not compatible with --save|-s").Build()
		} else if dataDir || multiDbDir {
			return errhand.BuildDError("Invalid Argument: --execute|-x is not compatible with --data-dir").Build()
		}
	}

	if save && (dataDir || multiDbDir) {
		return errhand.BuildDError("Invalid Argument: --data-dir queries cannot be saved").Build()
	}

	if query {
		if !save && msg {
			return errhand.BuildDError("Invalid Argument: --message|-m is only used with --query|-q and --save|-s").Build()
		}
	} else {
		if save {
			return errhand.BuildDError("Invalid Argument: --save|-s is only used with --query|-q").Build()
		}
		if msg {
			return errhand.BuildDError("Invalid Argument: --message|-m is only used with --query|-q and --save|-s").Build()
		}
	}

	if multiDbDir {
		cli.PrintErrln("WARNING: --multi-db-dir is deprecated, use --data-dir instead")
	}

	return nil
}

// execBatchMode runs all the queries in the input reader
func execBatchMode(ctx *sql.Context, qryist cli.Queryist, input io.Reader, continueOnErr bool, format engine.PrintResultFormat, binaryAsHex bool) error {
	scanner := NewStreamScanner(input)
	var query string
	for scanner.Scan() {
		if fileReadProg != nil {
			updateFileReadProgressOutput()
			fileReadProg.setReadBytes(int64(len(scanner.Bytes())))
		}
		query += scanner.Text()
		if len(query) == 0 || query == "\n" {
			continue
		}

		sqlMode := sql.LoadSqlMode(ctx)

		sqlStatement, err := sqlparser.ParseWithOptions(ctx, query, sqlMode.ParserOptions())
		if err == sqlparser.ErrEmpty {
			continue
		} else if err != nil {
			err = buildBatchSqlErr(scanner.state.statementStartLine, query, err)
			if !continueOnErr {
				return err
			} else {
				cli.PrintErrln(err.Error())
			}
		}

		// store start time for query
		ctx.SetQueryTime(time.Now())
		sqlSch, rowIter, _, err := processParsedQuery(ctx, query, qryist, sqlStatement)
		if err != nil {
			err = buildBatchSqlErr(scanner.state.statementStartLine, query, err)
			if !continueOnErr {
				return err
			} else {
				cli.PrintErrln(err.Error())
			}
		}

		if rowIter != nil {
			switch sqlStatement.(type) {
			case *sqlparser.Select, *sqlparser.Insert, *sqlparser.Update, *sqlparser.Delete,
				*sqlparser.OtherRead, *sqlparser.Show, *sqlparser.Explain, *sqlparser.SetOp:
				// For any statement that prints out result, print a newline to put the regular output on its own line
				if fileReadProg != nil {
					fileReadProg.printNewLineIfNeeded()
				}
			}
			err = engine.PrettyPrintResults(ctx, format, sqlSch, rowIter, false, false, false, binaryAsHex)
			if err != nil {
				err = buildBatchSqlErr(scanner.state.statementStartLine, query, err)
				if !continueOnErr {
					return err
				} else {
					cli.PrintErrln(err.Error())
				}
			}
		}
		query = ""
	}

	if err := scanner.Err(); err != nil {
		return buildBatchSqlErr(scanner.state.statementStartLine, query, err)
	}

	return nil
}

func buildBatchSqlErr(stmtStartLine int, query string, err error) error {
	return formatQueryError(fmt.Sprintf("error on line %d for query %s", stmtStartLine, query), err)
}

// execShell starts a SQL shell. Returns when the user exits the shell. The Root of the sqlEngine may
// be updated by any queries which were processed.
func execShell(sqlCtx *sql.Context, qryist cli.Queryist, format engine.PrintResultFormat, cliCtx cli.CliContext, binaryAsHex bool) error {
	_ = iohelp.WriteLine(cli.CliOut, welcomeMsg)
	historyFile := filepath.Join(".sqlhistory") // history file written to working dir

	db, branch, _ := getDBBranchFromSession(sqlCtx, qryist)
	dirty := false
	if branch != "" {
		dirty, _ = isDirty(sqlCtx, qryist)
	}

	initialPrompt, initialMultilinePrompt := formattedPrompts(db, branch, dirty)

	rlConf := readline.Config{
		Prompt:                 initialPrompt,
		Stdout:                 cli.CliOut,
		Stderr:                 cli.CliOut,
		HistoryFile:            historyFile,
		HistoryLimit:           500,
		HistorySearchFold:      true,
		DisableAutoSaveHistory: true,
	}

	verticalOutputLineTerminators := []string{"\\g", "\\G"}
	backSlashCommands := make([]string, 0, len(slashCmds))
	for _, cmd := range slashCmds {
		backSlashCommands = append(backSlashCommands, "\\"+cmd.Name())
	}

	shellConf := ishell.UninterpretedConfig{
		ReadlineConfig: &rlConf,
		QuitKeywords: []string{
			"quit", "exit", "quit()", "exit()",
		},
		LineTerminator:     ";",
		SpecialTerminators: verticalOutputLineTerminators,
		BackSlashCmds:      backSlashCommands,
	}

	shell := ishell.NewUninterpreted(&shellConf)
	shell.SetMultiPrompt(initialMultilinePrompt)
	// TODO: update completer on create / drop / alter statements
	completer, err := newCompleter(sqlCtx, qryist)
	if err != nil {
		return err
	}

	shell.CustomCompleter(completer)

	shell.EOF(func(c *ishell.Context) {
		c.Stop()
	})

	// The shell's interrupt handler handles an interrupt that occurs when it's accepting input. We also install our own
	// that handles interrupts during query execution or result printing, see below.
	shell.Interrupt(func(c *ishell.Context, count int, input string) {
		if count > 1 {
			c.Stop()
		} else {
			c.Println("Received SIGINT. Interrupt again to exit, or use ^D, quit, or exit")
		}
	})

	initialCtx := sqlCtx.Context

	//We want to gather the warnings if a server is running, as the connection queryist does not automatically cache them
	if c, ok := qryist.(cli.ShellServerQueryist); ok {
		c.EnableGatherWarnings()
	}

	toggleWarnings := true
	pagerEnabled := false
	// Used for the \edit command.
	lastSqlCmd := ""

	shell.Uninterpreted(func(c *ishell.Context) {
		query := c.Args[0]
		query = strings.TrimSpace(query)
		if len(query) == 0 {
			return
		}

		trackHistory(shell, query)

		query = strings.TrimSuffix(query, shell.LineTerminator())

		closureFormat := format
		// TODO: it would be better to build this into the statement parser rather than special case it here
		for _, terminator := range verticalOutputLineTerminators {
			if strings.HasSuffix(query, terminator) {
				closureFormat = engine.FormatVertical
			}
			query = strings.TrimSuffix(query, terminator)
		}

		var nextPrompt string
		var multiPrompt string
		cont := func() bool {
			subCtx, stop := signal.NotifyContext(initialCtx, os.Interrupt, syscall.SIGTERM)
			defer stop()

			var cancel func()
			sqlCtx, cancel = sqlCtx.NewSubContext()
			stopAfter := context.AfterFunc(subCtx, cancel)
			defer stopAfter()
			sqlCtx.SetQueryTime(time.Now())

			cmdType, subCmd, newQuery, err := preprocessQuery(query, lastSqlCmd, cliCtx)
			if err != nil {
				shell.Println(color.RedString(err.Error()))
				return true
			}

			if cmdType == DoltCliCommand {
				_, okOn := subCmd.(WarningOn)
				_, okOff := subCmd.(WarningOff)

				if _, ok := subCmd.(SlashPager); ok {
					p, err := handlePagerCommand(query)
					if err != nil {
						shell.Println(color.RedString(err.Error()))
					} else {
						pagerEnabled = p
					}
				} else if okOn || okOff {
					w, err := handleWarningCommand(query)
					if err != nil {
						shell.Println(color.RedString(err.Error()))
					} else {
						toggleWarnings = w
						if toggleWarnings {
							cli.Println("Show warnings enabled")
						} else {
							cli.Println("Show warnings disabled")
						}
					}
				} else {
					err := handleSlashCommand(sqlCtx, subCmd, query, cliCtx)
					if err != nil {
						shell.Println(color.RedString(err.Error()))
					}
				}
			} else {
				if cmdType == TransformCommand {
					query = newQuery
					trackHistory(shell, query+";")
				}
				lastSqlCmd = query
				sqlStmt, err := sqlparser.Parse(query)
				// silently skip empty statements
				if err == nil || err == sqlparser.ErrEmpty {
					sqlSch, rowIter, _, err := processParsedQuery(sqlCtx, query, qryist, sqlStmt)
					if err != nil {
						verr := formatQueryError("", err)
						shell.Println(verr.Verbose())
					} else if rowIter != nil {
						switch closureFormat {
						case engine.FormatTabular, engine.FormatVertical:
							err = engine.PrettyPrintResultsExtended(sqlCtx, closureFormat, sqlSch, rowIter, pagerEnabled, toggleWarnings, true, binaryAsHex)
						default:
							err = engine.PrettyPrintResults(sqlCtx, closureFormat, sqlSch, rowIter, pagerEnabled, toggleWarnings, true, binaryAsHex)
						}
					} else {
						if _, isUseStmt := sqlStmt.(*sqlparser.Use); isUseStmt {
							cli.Println("Database Changed")
						}
					}
				}
				if err != nil {
					shell.Println(color.RedString(err.Error()))
				}
			}

			nextPrompt, multiPrompt = postCommandUpdate(sqlCtx, qryist)

			return true
		}()

		if !cont {
			return
		}

		shell.SetPrompt(nextPrompt)
		shell.SetMultiPrompt(multiPrompt)
	})

	shell.Run()
	_ = iohelp.WriteLine(cli.CliOut, "Bye")

	return nil
}

func trackHistory(shell *ishell.Shell, query string) {
	// TODO: there's a bug in the readline library when editing multi-line history entries.
	// Longer term we need to switch to a new readline library, like in this bug:
	// https://github.com/cockroachdb/cockroach/issues/15460
	// For now, we store all history entries as single-line strings to avoid the issue.
	singleLine := strings.ReplaceAll(query, "\n", " ")

	if err := shell.AddHistory(singleLine); err != nil {
		// TODO: handle better, like by turning off history writing for the rest of the session
		shell.Println(color.RedString(err.Error()))
	}
}

type CommandType int

// CommandType is used to determine how to handle a query. See preprocessQuery.
const (
	DoltCliCommand CommandType = iota
	SqlShellCommand
	TransformCommand
)

// preprocessQuery takes the user's query and returns the command type, the command, and the query to execute. The
// CommandType returned is going to be used to determine how to handle the query.
//   - DoltCliCommand: the cli.Command returned should be executed. Query string is empty, and should be ignored.
//   - TransformCommand: The 'lastQuery' argument will be transformed into something else, using the EDITOR.
//     The query returned will be the edited query, and should be entered into the user's command history. The cli.Command will be nil.
//   - SqlShellCommand: cli.Command will be nil. The query returned will be identical to the query passed in.
func preprocessQuery(query, lastQuery string, cliCtx cli.CliContext) (CommandType, cli.Command, string, error) {
	// strip leading whitespace
	query = strings.TrimLeft(query, " \t\n\r\v\f")
	if strings.HasPrefix(query, "\\") {
		if query == "\\edit" {
			// \edit is a special case. Maybe we'll generalize this in the future.
			updatedQuery, err := execEditor(lastQuery, ".sql", cliCtx)
			if err != nil {
				return TransformCommand, nil, "", err
			}
			// Trailing newlines are common in editors, so may as well trim all whitespace.
			updatedQuery = strings.TrimRight(updatedQuery, " \t\n\r\v\f")
			return TransformCommand, nil, updatedQuery, nil
		}

		cmd, ok := findSlashCmd(query[1:])
		if ok {
			return DoltCliCommand, cmd, "", nil
		}
	}
	return SqlShellCommand, nil, query, nil
}

// postCommandUpdate is a helper function that is run after the shell has completed a command. It updates the the database
// if needed, and generates new prompts for the shell (based on the branch and if the workspace is dirty).
func postCommandUpdate(sqlCtx *sql.Context, qryist cli.Queryist) (string, string) {
	db, branch, ok := getDBBranchFromSession(sqlCtx, qryist)
	if ok {
		sqlCtx.SetCurrentDatabase(db)
	}
	dirty := false
	if branch != "" {
		dirty, _ = isDirty(sqlCtx, qryist)
	}
	return formattedPrompts(db, branch, dirty)
}

// formattedPrompts returns the prompt and multiline prompt for the current session. If the db is empty, the prompt will
// be "> ", otherwise it will be "db> ". If the branch is empty, the multiline prompt will be "-> ", left padded for
// alignment with the prompt.
func formattedPrompts(db, branch string, dirty bool) (string, string) {
	if db == "" {
		return "> ", "-> "
	}
	if branch == "" {
		// +2 Allows for the "->" to lineup correctly
		multi := fmt.Sprintf(fmt.Sprintf("%%%ds", len(db)+2), "-> ")
		cyanDb := color.CyanString(db)
		return fmt.Sprintf("%s> ", cyanDb), multi
	}

	// +3 is for the "/" and "->" to lineup correctly
	promptLen := len(db) + len(branch) + 3
	dirtyStr := ""
	if dirty {
		dirtyStr = color.RedString("*")
		promptLen += 1
	}

	multi := fmt.Sprintf(fmt.Sprintf("%%%ds", promptLen), "-> ")

	cyanDb := color.CyanString(db)
	yellowBr := color.YellowString(branch)
	return fmt.Sprintf("%s/%s%s> ", cyanDb, yellowBr, dirtyStr), multi
}

// getDBBranchFromSession returns the current database name and current branch  for the session, handling all the errors
// along the way by printing red error messages to the CLI. If there was an issue getting the db name, the ok return
// value will be false and the strings will be empty.
func getDBBranchFromSession(sqlCtx *sql.Context, qryist cli.Queryist) (db string, branch string, ok bool) {
	_, _, _, err := qryist.Query(sqlCtx, "set lock_warnings = 1")
	if err != nil {
		cli.Println(color.RedString(err.Error()))
		return "", "", false
	}
	defer qryist.Query(sqlCtx, "set lock_warnings = 0")

	_, resp, _, err := qryist.Query(sqlCtx, "select database() as db, active_branch() as branch")
	if err != nil {
		cli.Println(color.RedString("Failure to get DB Name for session: " + err.Error()))
		return db, branch, false
	}
	// Expect single row result, with two columns: db name, branch name.
	row, err := resp.Next(sqlCtx)
	if err != nil {
		cli.Println(color.RedString("Failure to get DB Name for session: " + err.Error()))
		return db, branch, false
	}
	if len(row) != 2 {
		cli.Println(color.RedString("Runtime error. Invalid column count."))
		return db, branch, false
	}

	if row[1] == nil {
		branch = ""
	} else {
		branch = row[1].(string)
	}
	if row[0] == nil {
		db = ""
	} else {
		db = row[0].(string)

		// It is possible to `use mydb/branch`, and as far as your session is concerned your database is mydb/branch. We
		// allow that, but also want to show the user the branch name in the prompt. So we munge the DB in this case.
		if strings.HasSuffix(strings.ToLower(db), strings.ToLower("/"+branch)) {
			db = db[:len(db)-len(branch)-1]
		}
	}

	return db, branch, true
}

// isDirty returns true if the workspace is dirty, false otherwise. This function _assumes_ you are on a database
// with a branch. If you are not, you will get an error.
func isDirty(sqlCtx *sql.Context, qryist cli.Queryist) (bool, error) {
	_, _, _, err := qryist.Query(sqlCtx, "set lock_warnings = 1")
	if err != nil {
		return false, err
	}
	defer qryist.Query(sqlCtx, "set lock_warnings = 0")

	_, resp, _, err := qryist.Query(sqlCtx, "select count(table_name) > 0 as dirty from dolt_status")

	if err != nil {
		cli.Println(color.RedString("Failure to get DB Name for session: " + err.Error()))
		return false, err
	}
	// Expect single row result, with one boolean column.
	row, err := resp.Next(sqlCtx)
	if err != nil {
		cli.Println(color.RedString("Failure to get DB Name for session: " + err.Error()))
		return false, err
	}
	if len(row) != 1 {
		cli.Println(color.RedString("Runtime error. Invalid column count."))
		return false, fmt.Errorf("invalid column count")
	}

	return getStrBoolColAsBool(row[0])
}

// Returns a new auto completer with table names, column names, and SQL keywords.
// TODO: update the completer on DDL, branch change, etc.
func newCompleter(
	ctx *sql.Context,
	qryist cli.Queryist,
) (completer *sqlCompleter, rerr error) {
	subCtx, stop := signal.NotifyContext(ctx.Context, os.Interrupt, syscall.SIGTERM)
	defer stop()

	sqlCtx := sql.NewContext(subCtx, sql.WithSession(ctx.Session))

	sqlCtx.Session.LockWarnings()
	defer sqlCtx.Session.UnlockWarnings()
	_, iter, _, err := qryist.Query(sqlCtx, "select table_schema, table_name, column_name from information_schema.columns;")
	if err != nil {
		return nil, err
	}

	defer func(iter sql.RowIter, context *sql.Context) {
		err := iter.Close(context)
		if err != nil && rerr == nil {
			rerr = err
		}
	}(iter, sqlCtx)

	identifiers := make(map[string]struct{})
	var columnNames []string
	for {
		r, err := iter.Next(sqlCtx)
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}

		identifiers[r[0].(string)] = struct{}{}
		identifiers[r[1].(string)] = struct{}{}
		identifiers[r[2].(string)] = struct{}{}
		columnNames = append(columnNames, r[2].(string))
	}

	var completionWords []string
	for k := range identifiers {
		completionWords = append(completionWords, k)
	}

	completionWords = append(completionWords, dsqle.CommonKeywords...)

	return &sqlCompleter{
		allWords:    completionWords,
		columnNames: columnNames,
	}, nil
}

type sqlCompleter struct {
	allWords    []string
	columnNames []string
}

// Do function for autocompletion, defined by the Readline library. Mostly stolen from ishell.
func (c *sqlCompleter) Do(line []rune, pos int) (newLine [][]rune, length int) {
	var words []string
	if w, err := shlex.Split(string(line)); err == nil {
		words = w
	} else {
		// fall back
		words = strings.Fields(string(line))
	}

	var cWords []string
	prefix := ""
	lastWord := ""
	if len(words) > 0 && pos > 0 && line[pos-1] != ' ' {
		lastWord = words[len(words)-1]
		prefix = strings.ToLower(lastWord)
	} else if len(words) > 0 {
		lastWord = words[len(words)-1]
	}

	cWords = c.getWords(lastWord)

	var suggestions [][]rune
	for _, w := range cWords {
		lowered := strings.ToLower(w)
		if strings.HasPrefix(lowered, prefix) {
			suggestions = append(suggestions, []rune(strings.TrimPrefix(lowered, prefix)))
		}
	}
	if len(suggestions) == 1 && prefix != "" && string(suggestions[0]) == "" {
		suggestions = [][]rune{[]rune(" ")}
	}

	return suggestions, len(prefix)
}

// Simple suggestion function. Returns column name suggestions if the last word in the input has exactly one '.' in it,
// otherwise returns all tables, columns, and reserved words.
func (c *sqlCompleter) getWords(lastWord string) (s []string) {
	lastDot := strings.LastIndex(lastWord, ".")
	if lastDot > 0 && strings.Count(lastWord, ".") == 1 {
		alias := lastWord[:lastDot]
		return prepend(alias+".", c.columnNames)
	}

	return c.allWords
}

func prepend(s string, ss []string) []string {
	newSs := make([]string, len(ss))
	for i := range ss {
		newSs[i] = s + ss[i]
	}
	return newSs
}

// processQuery processes a single query. The Root of the sqlEngine will be updated if necessary.
// Returns the schema and the row iterator for the results, which may be nil, and an error if one occurs.
func processQuery(ctx *sql.Context, query string, qryist cli.Queryist) (sql.Schema, sql.RowIter, *sql.QueryFlags, error) {
	sqlStatement, err := sqlparser.Parse(query)
	if err == sqlparser.ErrEmpty {
		// silently skip empty statements
		return nil, nil, nil, nil
	} else if err != nil {
		return nil, nil, nil, err
	}
	return processParsedQuery(ctx, query, qryist, sqlStatement)
}

// processParsedQuery processes a single query with the parsed statement provided. The Root of the sqlEngine
// will be updated if necessary. Returns the schema and the row iterator for the results, which may be nil,
// and an error if one occurs.
func processParsedQuery(ctx *sql.Context, query string, qryist cli.Queryist, sqlStatement sqlparser.Statement) (sql.Schema, sql.RowIter, *sql.QueryFlags, error) {
	switch s := sqlStatement.(type) {
	case *sqlparser.Use, *sqlparser.Commit:
		_, ri, _, err := qryist.Query(ctx, query)
		if err != nil {
			return nil, nil, nil, err
		}
		_, err = sql.RowIterToRows(ctx, ri)
		if err != nil {
			return nil, nil, nil, err
		}
		return nil, nil, nil, nil
	case *sqlparser.Insert, *sqlparser.Update, *sqlparser.Delete,
		*sqlparser.AlterTable, *sqlparser.DDL, *sqlparser.Set:
		sch, ri, _, err := qryist.Query(ctx, query)
		if err != nil {
			return nil, nil, nil, err
		}
		rows, err := sql.RowIterToRows(ctx, ri)
		if err != nil {
			return nil, nil, nil, err
		}
		newRowIter := sql.RowsToRowIter(rows...)
		return sch, newRowIter, nil, nil
	case *sqlparser.Load:
		if s.Local {
			return nil, nil, nil, fmt.Errorf("LOCAL supported only in sql-server mode")
		}
		sch, ri, _, err := qryist.Query(ctx, query)
		if err != nil {
			return nil, nil, nil, err
		}
		rows, err := sql.RowIterToRows(ctx, ri)
		if err != nil {
			return nil, nil, nil, err
		}
		newRowIter := sql.RowsToRowIter(rows...)
		return sch, newRowIter, nil, nil
	default:
		return qryist.QueryWithBindings(ctx, query, sqlStatement, nil, nil)
	}
}

type stats struct {
	rowsInserted   int
	rowsUpdated    int
	rowsDeleted    int
	unflushedEdits int
	unprintedEdits int
	displayStrLen  int
}

type fileReadProgress struct {
	bytesRead     int64
	totalBytes    int64
	printed       int64
	displayStrLen int
}

var batchEditStats = &stats{}
var fileReadProg *fileReadProgress

const maxBatchSize = 200000
const updateInterval = 1000

func (s *stats) numUpdates() int {
	return s.rowsUpdated + s.rowsDeleted + s.rowsInserted
}

func (s *stats) shouldUpdateBatchModeOutput() bool {
	return s.unprintedEdits >= updateInterval
}

func (s *stats) shouldFlush() bool {
	return s.unflushedEdits >= maxBatchSize
}

// printNewLineIfNeeded prints a new line when there are outputs printed other than its output line of batch read progress.
func (s *stats) printNewLineIfNeeded() {
	if s.displayStrLen > 0 {
		cli.Print("\n")
		s.displayStrLen = 0
	}
}

// close will print last updated line of processed 100.0% and a new line
func (f *fileReadProgress) close() {
	f.bytesRead = f.totalBytes
	updateFileReadProgressOutput()
	cli.Println() // need a newline after all updates are executed
}

// setReadBytes updates number of bytes that are read so far from the file
func (f *fileReadProgress) setReadBytes(b int64) {
	f.bytesRead = f.printed + b
}

// printNewLineIfNeeded prints a new line when there are outputs printed other than its output line of file read progress.
func (f *fileReadProgress) printNewLineIfNeeded() {
	if f.displayStrLen > 0 {
		cli.Print("\n")
		f.displayStrLen = 0
	}
}

// updateFileReadProgressOutput will delete the line it printed before, and print the updated line.
// If there were other functions printed result, it will print update line on a new line.
// This function is used for only file reads for dolt sql when `--file` flag is used.
func updateFileReadProgressOutput() {
	if fileReadProg == nil {
		// this should not happen, but sanity check
		cli.PrintErrln("No file is being processed.")
	}
	// batch can be writing to the line, so print new line.
	batchEditStats.printNewLineIfNeeded()
	percent := float64(fileReadProg.bytesRead) / float64(fileReadProg.totalBytes) * 100
	fileReadProg.printed = fileReadProg.bytesRead
	displayStr := fmt.Sprintf("Processed %.1f%% of the file", percent)
	fileReadProg.displayStrLen = cli.DeleteAndPrint(fileReadProg.displayStrLen, displayStr)
}
