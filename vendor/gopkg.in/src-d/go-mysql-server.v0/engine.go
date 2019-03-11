package sqle // import "gopkg.in/src-d/go-mysql-server.v0"

import (
	"strings"

	opentracing "github.com/opentracing/opentracing-go"
	"github.com/sirupsen/logrus"
	"gopkg.in/src-d/go-mysql-server.v0/auth"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/analyzer"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression/function"
	"gopkg.in/src-d/go-mysql-server.v0/sql/parse"
	"gopkg.in/src-d/go-mysql-server.v0/sql/plan"
)

// Config for the Engine.
type Config struct {
	// VersionPostfix to display with the `VERSION()` UDF.
	VersionPostfix string
	// Auth used for authentication and authorization.
	Auth auth.Auth
}

// Engine is a SQL engine.
type Engine struct {
	Catalog  *sql.Catalog
	Analyzer *analyzer.Analyzer
	Auth     auth.Auth
}

// New creates a new Engine with custom configuration. To create an Engine with
// the default settings use `NewDefault`.
func New(c *sql.Catalog, a *analyzer.Analyzer, cfg *Config) *Engine {
	var versionPostfix string
	if cfg != nil {
		versionPostfix = cfg.VersionPostfix
	}

	c.RegisterFunctions(function.Defaults)
	c.RegisterFunction("version", sql.FunctionN(function.NewVersion(versionPostfix)))
	c.RegisterFunction("database", sql.Function0(function.NewDatabase(c)))

	// use auth.None if auth is not specified
	var au auth.Auth
	if cfg == nil || cfg.Auth == nil {
		au = new(auth.None)
	} else {
		au = cfg.Auth
	}

	return &Engine{c, a, au}
}

// NewDefault creates a new default Engine.
func NewDefault() *Engine {
	c := sql.NewCatalog()
	a := analyzer.NewDefault(c)

	return New(c, a, nil)
}

// Query executes a query.
func (e *Engine) Query(
	ctx *sql.Context,
	query string,
) (sql.Schema, sql.RowIter, error) {
	span, ctx := ctx.Span("query", opentracing.Tag{Key: "query", Value: query})
	defer span.Finish()

	if strings.ToLower(query) == "current_user()" {
		return getUser(ctx.Client().User)
	} else if strings.ToLower(query) == "show status" {
		return getStatus(false, filterNone)
	} else if strings.ToLower(query) == "show global status" {
		return getStatus(false, filterNone)
	} else if strings.HasPrefix(strings.ToLower(query), "show session status like") {
		// TODO: get content of LIKE filter
		return getStatus(false, "Ssl_cipher")
	} else if strings.ToLower(query) == "show engines" {
		return getEngines()
	}

	logrus.WithField("query", query).Debug("executing query")

	parsed, err := parse.Parse(ctx, query)
	if err != nil {
		return nil, nil, err
	}

	var perm = auth.ReadPerm
	var typ = sql.QueryProcess
	switch parsed.(type) {
	case *plan.CreateIndex:
		typ = sql.CreateIndexProcess
		perm = auth.ReadPerm | auth.WritePerm
	case *plan.InsertInto, *plan.DropIndex, *plan.UnlockTables, *plan.LockTables:
		perm = auth.ReadPerm | auth.WritePerm
	}

	err = e.Auth.Allowed(ctx, perm)
	if err != nil {
		return nil, nil, err
	}

	ctx, err = e.Catalog.AddProcess(ctx, typ, query)
	defer func() {
		if err != nil && ctx != nil {
			e.Catalog.Done(ctx.Pid())
		}
	}()

	if err != nil {
		return nil, nil, err
	}

	analyzed, err := e.Analyzer.Analyze(ctx, parsed)
	if err != nil {
		return nil, nil, err
	}

	iter, err := analyzed.RowIter(ctx)
	if err != nil {
		return nil, nil, err
	}

	return analyzed.Schema(), iter, nil
}

// AddDatabase adds the given database to the catalog.
func (e *Engine) AddDatabase(db sql.Database) {
	e.Catalog.AddDatabase(db)
}

// Init performs all the initialization requirements for the engine to work.
func (e *Engine) Init() error {
	return e.Catalog.LoadIndexes(e.Catalog.AllDatabases())
}

func getUser(user string) (sql.Schema, sql.RowIter, error) {
	var cols []*sql.Column
	cols = append(cols, &sql.Column{Name: "CURRENT_USER()", Type: sql.Text})

	var rows []sql.Row
	rows = append(rows, sql.NewRow([]byte(user)))

	return sql.Schema(cols), sql.RowsToRowIter(rows...), nil
}

func getEngines() (sql.Schema, sql.RowIter, error) {
	var cols []*sql.Column
	cols = append(cols, &sql.Column{Name: "Engine", Type: sql.Text})
	cols = append(cols, &sql.Column{Name: "Support", Type: sql.Text})
	cols = append(cols, &sql.Column{Name: "Comment", Type: sql.Text})
	cols = append(cols, &sql.Column{Name: "Transactions", Type: sql.Text})
	cols = append(cols, &sql.Column{Name: "XA", Type: sql.Text})
	cols = append(cols, &sql.Column{Name: "Savepoints", Type: sql.Text})

	var rows []sql.Row
	rows = append(rows, sql.NewRow([]byte("MyISAM"), "YES", []byte("MyISAM storage engine"), "NO", "NO", "NO"))
	rows = append(rows, sql.NewRow([]byte("CSV"), "YES", []byte("CSV storage engine"), "NO", "NO", "NO"))
	rows = append(rows, sql.NewRow([]byte("MRG_MYISAM"), "YES", []byte("Collection of identical MyISAM tables"), "NO", "NO", "NO"))
	rows = append(rows, sql.NewRow([]byte("BLACKHOLE"), "YES", []byte("/dev/null"), "NO", "NO", "NO"))
	rows = append(rows, sql.NewRow([]byte("PERFORMANCE_SCHEMA"), "YES", []byte("Performance Schema"), "NO", "NO", "NO"))
	rows = append(rows, sql.NewRow([]byte("InnoDB"), "YES", []byte("Supports transactions, row-level locking, and foreign keys"), "YES", "YES", "YES"))
	rows = append(rows, sql.NewRow([]byte("ARCHIVE"), "YES", []byte("Archive storage engine"), "NO", "NO", "NO"))
	rows = append(rows, sql.NewRow([]byte("MEMORY"), "YES", []byte("Hash based, stored in memory, useful for temporary tables"), "NO", "NO", "NO"))
	rows = append(rows, sql.NewRow([]byte("FEDERATED"), "NO", []byte("Federated MySQL storage engine"), "NO", "NO", "NO"))

	return sql.Schema(cols), sql.RowsToRowIter(rows...), nil
}

/*
+--------------------+---------+----------------------------------------------------------------+--------------+------+------------+
| Engine             | Support | Comment                                                        | Transactions | XA   | Savepoints |
+--------------------+---------+----------------------------------------------------------------+--------------+------+------------+
| MyISAM             | YES     | MyISAM storage engine                                          | NO           | NO   | NO         |
| CSV                | YES     | CSV storage engine                                             | NO           | NO   | NO         |
| MRG_MYISAM         | YES     | Collection of identical MyISAM tables                          | NO           | NO   | NO         |
| BLACKHOLE          | YES     | /dev/null storage engine (anything you write to it disappears) | NO           | NO   | NO         |
| PERFORMANCE_SCHEMA | YES     | Performance Schema                                             | NO           | NO   | NO         |
| InnoDB             | DEFAULT | Supports transactions, row-level locking, and foreign keys     | YES          | YES  | YES        |
| ARCHIVE            | YES     | Archive storage engine                                         | NO           | NO   | NO         |
| MEMORY             | YES     | Hash based, stored in memory, useful for temporary tables      | NO           | NO   | NO         |
| FEDERATED          | NO      | Federated MySQL storage engine                                 | NULL         | NULL | NULL       |
+--------------------+---------+----------------------------------------------------------------+--------------+------+------------+
*/

const filterNone = "none"

func getStatus(isExample bool, filter string) (sql.Schema, sql.RowIter, error) {
	var cols []*sql.Column
	cols = append(cols, &sql.Column{Name: "Variable_name", Type: sql.Text})
	cols = append(cols, &sql.Column{Name: "Value", Type: sql.Int64})

	var status []statusRow
	if filter != filterNone && isExample {
		status = append(status, statusRow{"Aborted_clients", 0})
		status = append(status, statusRow{"Aborted_connects", 0})
		status = append(status, statusRow{"Bytes_received", 0})
		status = append(status, statusRow{"Bytes_sent", 0})
		status = append(status, statusRow{"Connections", 0})
		status = append(status, statusRow{"Open_tables", 0})
		status = append(status, statusRow{"Opened_tables", 0})
		status = append(status, statusRow{"Uptime", 1})
	} else {
		status = append(status, statusRow{"Aborted_clients", 0})
		status = append(status, statusRow{"Aborted_connects", 31})
		status = append(status, statusRow{"Binlog_cache_disk_use", 0})
		status = append(status, statusRow{"Binlog_cache_use", 0})
		status = append(status, statusRow{"Binlog_stmt_cache_disk_use", 0})
		status = append(status, statusRow{"Binlog_stmt_cache_use", 0})
		status = append(status, statusRow{"Bytes_received", 839})
		status = append(status, statusRow{"Bytes_sent", 12128})
		status = append(status, statusRow{"Com_admin_commands", 0})
		status = append(status, statusRow{"Com_assign_to_keycache", 0})
		status = append(status, statusRow{"Com_alter_db", 0})
		status = append(status, statusRow{"Com_alter_db_upgrade", 0})
		status = append(status, statusRow{"Com_alter_event", 0})
		status = append(status, statusRow{"Com_alter_function", 0})
		status = append(status, statusRow{"Com_alter_instance", 0})
		status = append(status, statusRow{"Com_alter_procedure", 0})
		status = append(status, statusRow{"Com_alter_server", 0})
		status = append(status, statusRow{"Com_alter_table", 0})
		status = append(status, statusRow{"Com_alter_tablespace", 0})
		status = append(status, statusRow{"Com_alter_user", 0})
		status = append(status, statusRow{"Com_analyze", 0})
		status = append(status, statusRow{"Com_begin", 0})
		status = append(status, statusRow{"Com_binlog", 0})
		status = append(status, statusRow{"Com_call_procedure", 0})
		status = append(status, statusRow{"Com_change_db", 1})
		status = append(status, statusRow{"Com_change_master", 0})
		status = append(status, statusRow{"Com_change_repl_filter", 0})
		status = append(status, statusRow{"Com_check", 0})
		status = append(status, statusRow{"Com_checksum", 0})
		status = append(status, statusRow{"Com_commit", 0})
		status = append(status, statusRow{"Com_create_db", 0})
		status = append(status, statusRow{"Com_create_event", 0})
		status = append(status, statusRow{"Com_create_function", 0})
		status = append(status, statusRow{"Com_create_index", 0})
		status = append(status, statusRow{"Com_create_procedure", 0})
		status = append(status, statusRow{"Com_create_server", 0})
		status = append(status, statusRow{"Com_create_table", 0})
		status = append(status, statusRow{"Com_create_trigger", 0})
		status = append(status, statusRow{"Com_create_udf", 0})
		status = append(status, statusRow{"Com_create_user", 0})
		status = append(status, statusRow{"Com_create_view", 0})
		status = append(status, statusRow{"Com_dealloc_sql", 0})
		status = append(status, statusRow{"Com_delete", 0})
		status = append(status, statusRow{"Com_delete_multi", 0})
		status = append(status, statusRow{"Com_do", 0})
		status = append(status, statusRow{"Com_drop_db", 0})
		status = append(status, statusRow{"Com_drop_event", 0})
		status = append(status, statusRow{"Com_drop_function", 0})
		status = append(status, statusRow{"Com_drop_index", 0})
		status = append(status, statusRow{"Com_drop_procedure", 0})
		status = append(status, statusRow{"Com_drop_server", 0})
		status = append(status, statusRow{"Com_drop_table", 0})
		status = append(status, statusRow{"Com_drop_trigger", 0})
		status = append(status, statusRow{"Com_drop_user", 0})
		status = append(status, statusRow{"Com_drop_view", 0})
		status = append(status, statusRow{"Com_empty_query", 0})
		status = append(status, statusRow{"Com_execute_sql", 0})
		status = append(status, statusRow{"Com_explain_other", 0})
		status = append(status, statusRow{"Com_flush", 0})
		status = append(status, statusRow{"Com_get_diagnostics", 0})
		status = append(status, statusRow{"Com_grant", 0})
		status = append(status, statusRow{"Com_ha_close", 0})
		status = append(status, statusRow{"Com_ha_open", 0})
		status = append(status, statusRow{"Com_ha_read", 0})
		status = append(status, statusRow{"Com_help", 0})
		status = append(status, statusRow{"Com_insert", 0})
		status = append(status, statusRow{"Com_insert_select", 0})
		status = append(status, statusRow{"Com_install_plugin", 0})
		status = append(status, statusRow{"Com_kill", 0})
		status = append(status, statusRow{"Com_load", 0})
		status = append(status, statusRow{"Com_lock_tables", 0})
		status = append(status, statusRow{"Com_optimize", 0})
		status = append(status, statusRow{"Com_preload_keys", 0})
		status = append(status, statusRow{"Com_prepare_sql", 0})
		status = append(status, statusRow{"Com_purge", 0})
		status = append(status, statusRow{"Com_purge_before_date", 0})
		status = append(status, statusRow{"Com_release_savepoint", 0})
		status = append(status, statusRow{"Com_rename_table", 0})
		status = append(status, statusRow{"Com_rename_user", 0})
		status = append(status, statusRow{"Com_repair", 0})
		status = append(status, statusRow{"Com_replace", 0})
		status = append(status, statusRow{"Com_replace_select", 0})
		status = append(status, statusRow{"Com_reset", 0})
		status = append(status, statusRow{"Com_resignal", 0})
		status = append(status, statusRow{"Com_revoke", 0})
		status = append(status, statusRow{"Com_revoke_all", 0})
		status = append(status, statusRow{"Com_rollback", 0})
		status = append(status, statusRow{"Com_rollback_to_savepoint", 0})
		status = append(status, statusRow{"Com_savepoint", 0})
		status = append(status, statusRow{"Com_select", 3})
		status = append(status, statusRow{"Com_set_option", 6})
		status = append(status, statusRow{"Com_signal", 0})
		status = append(status, statusRow{"Com_show_binlog_events", 0})
		status = append(status, statusRow{"Com_show_binlogs", 0})
		status = append(status, statusRow{"Com_show_charsets", 0})
		status = append(status, statusRow{"Com_show_collations", 0})
		status = append(status, statusRow{"Com_show_create_db", 0})
		status = append(status, statusRow{"Com_show_create_event", 0})
		status = append(status, statusRow{"Com_show_create_func", 0})
		status = append(status, statusRow{"Com_show_create_proc", 0})
		status = append(status, statusRow{"Com_show_create_table", 0})
		status = append(status, statusRow{"Com_show_create_trigger", 0})
		status = append(status, statusRow{"Com_show_databases", 0})
		status = append(status, statusRow{"Com_show_engine_logs", 0})
		status = append(status, statusRow{"Com_show_engine_mutex", 0})
		status = append(status, statusRow{"Com_show_engine_status", 0})
		status = append(status, statusRow{"Com_show_events", 0})
		status = append(status, statusRow{"Com_show_errors", 0})
		status = append(status, statusRow{"Com_show_fields", 0})
		status = append(status, statusRow{"Com_show_function_code", 0})
		status = append(status, statusRow{"Com_show_function_status", 0})
		status = append(status, statusRow{"Com_show_grants", 0})
		status = append(status, statusRow{"Com_show_keys", 0})
		status = append(status, statusRow{"Com_show_master_status", 0})
		status = append(status, statusRow{"Com_show_open_tables", 0})
		status = append(status, statusRow{"Com_show_plugins", 0})
		status = append(status, statusRow{"Com_show_privileges", 0})
		status = append(status, statusRow{"Com_show_procedure_code", 0})
		status = append(status, statusRow{"Com_show_procedure_status", 0})
		status = append(status, statusRow{"Com_show_processlist", 0})
		status = append(status, statusRow{"Com_show_profile", 0})
		status = append(status, statusRow{"Com_show_profiles", 0})
		status = append(status, statusRow{"Com_show_relaylog_events", 0})
		status = append(status, statusRow{"Com_show_slave_hosts", 0})
		status = append(status, statusRow{"Com_show_slave_status", 0})
		status = append(status, statusRow{"Com_show_status", 3})
		status = append(status, statusRow{"Com_show_storage_engines", 0})
		status = append(status, statusRow{"Com_show_table_status", 0})
		status = append(status, statusRow{"Com_show_tables", 0})
		status = append(status, statusRow{"Com_show_triggers", 0})
		status = append(status, statusRow{"Com_show_variables", 6})
		status = append(status, statusRow{"Com_show_warnings", 0})
		status = append(status, statusRow{"Com_show_create_user", 0})
		status = append(status, statusRow{"Com_shutdown", 0})
		status = append(status, statusRow{"Com_slave_start", 0})
		status = append(status, statusRow{"Com_slave_stop", 0})
		status = append(status, statusRow{"Com_group_replication_start", 0})
		status = append(status, statusRow{"Com_group_replication_stop", 0})
		status = append(status, statusRow{"Com_stmt_execute", 0})
		status = append(status, statusRow{"Com_stmt_close", 0})
		status = append(status, statusRow{"Com_stmt_fetch", 0})
		status = append(status, statusRow{"Com_stmt_prepare", 0})
		status = append(status, statusRow{"Com_stmt_reset", 0})
		status = append(status, statusRow{"Com_stmt_send_long_data", 0})
		status = append(status, statusRow{"Com_truncate", 0})
		status = append(status, statusRow{"Com_uninstall_plugin", 0})
		status = append(status, statusRow{"Com_unlock_tables", 0})
		status = append(status, statusRow{"Com_update", 0})
		status = append(status, statusRow{"Com_update_multi", 0})
		status = append(status, statusRow{"Com_xa_commit", 0})
		status = append(status, statusRow{"Com_xa_end", 0})
		status = append(status, statusRow{"Com_xa_prepare", 0})
		status = append(status, statusRow{"Com_xa_recover", 0})
		status = append(status, statusRow{"Com_xa_rollback", 0})
		status = append(status, statusRow{"Com_xa_start", 0})
		status = append(status, statusRow{"Com_stmt_reprepare", 0})
		status = append(status, statusRow{"Compression", 0}) // TODO: OFF
		status = append(status, statusRow{"Connection_errors_accept", 0})
		status = append(status, statusRow{"Connection_errors_internal", 0})
		status = append(status, statusRow{"Connection_errors_max_connections", 0})
		status = append(status, statusRow{"Connection_errors_peer_address", 0})
		status = append(status, statusRow{"Connection_errors_select", 0})
		status = append(status, statusRow{"Connection_errors_tcpwrap", 0})
		status = append(status, statusRow{"Connections", 53})
		status = append(status, statusRow{"Created_tmp_disk_tables", 0})
		status = append(status, statusRow{"Created_tmp_files", 6})
		status = append(status, statusRow{"Created_tmp_tables", 6})
		status = append(status, statusRow{"Delayed_errors", 0})
		status = append(status, statusRow{"Delayed_insert_threads", 0})
		status = append(status, statusRow{"Delayed_writes", 0})
		status = append(status, statusRow{"Flush_commands", 1})
		status = append(status, statusRow{"Handler_commit", 0})
		status = append(status, statusRow{"Handler_delete", 0})
		status = append(status, statusRow{"Handler_discover", 0})
		status = append(status, statusRow{"Handler_external_lock", 12})
		status = append(status, statusRow{"Handler_mrr_init", 0})
		status = append(status, statusRow{"Handler_prepare", 0})
		status = append(status, statusRow{"Handler_read_first", 0})
		status = append(status, statusRow{"Handler_read_key", 0})
		status = append(status, statusRow{"Handler_read_last", 0})
		status = append(status, statusRow{"Handler_read_next", 0})
		status = append(status, statusRow{"Handler_read_prev", 0})
		status = append(status, statusRow{"Handler_read_rnd", 0})
		status = append(status, statusRow{"Handler_read_rnd_next", 6252})
		status = append(status, statusRow{"Handler_rollback", 0})
		status = append(status, statusRow{"Handler_savepoint", 0})
		status = append(status, statusRow{"Handler_savepoint_rollback", 0})
		status = append(status, statusRow{"Handler_update", 0})
		status = append(status, statusRow{"Handler_write", 3120})
		status = append(status, statusRow{"Innodb_buffer_pool_dump_status", 0}) // TODO: Dumping of buffer pool not started
		status = append(status, statusRow{"Innodb_buffer_pool_load_status", 0}) // TODO: Buffer pool(s) load completed at 190228  9:05:28
		status = append(status, statusRow{"Innodb_buffer_pool_resize_status", 0})
		status = append(status, statusRow{"Innodb_buffer_pool_pages_data", 429})
		status = append(status, statusRow{"Innodb_buffer_pool_bytes_data", 7028736})
		status = append(status, statusRow{"Innodb_buffer_pool_pages_dirty", 0})
		status = append(status, statusRow{"Innodb_buffer_pool_bytes_dirty", 0})
		status = append(status, statusRow{"Innodb_buffer_pool_pages_flushed", 39})
		status = append(status, statusRow{"Innodb_buffer_pool_pages_free", 7762})
		status = append(status, statusRow{"Innodb_buffer_pool_pages_misc", 0})
		status = append(status, statusRow{"Innodb_buffer_pool_pages_total", 8191})
		status = append(status, statusRow{"Innodb_buffer_pool_read_ahead_rnd", 0})
		status = append(status, statusRow{"Innodb_buffer_pool_read_ahead", 0})
		status = append(status, statusRow{"Innodb_buffer_pool_read_ahead_evicted", 0})
		status = append(status, statusRow{"Innodb_buffer_pool_read_requests", 1715})
		status = append(status, statusRow{"Innodb_buffer_pool_reads", 395})
		status = append(status, statusRow{"Innodb_buffer_pool_wait_free", 0})
		status = append(status, statusRow{"Innodb_buffer_pool_write_requests", 425})
		status = append(status, statusRow{"Innodb_data_fsyncs", 7})
		status = append(status, statusRow{"Innodb_data_pending_fsyncs", 0})
		status = append(status, statusRow{"Innodb_data_pending_reads", 0})
		status = append(status, statusRow{"Innodb_data_pending_writes", 0})
		status = append(status, statusRow{"Innodb_data_read", 6541824})
		status = append(status, statusRow{"Innodb_data_reads", 425})
		status = append(status, statusRow{"Innodb_data_writes", 56})
		status = append(status, statusRow{"Innodb_data_written", 673792})
		status = append(status, statusRow{"Innodb_dblwr_pages_written", 2})
		status = append(status, statusRow{"Innodb_dblwr_writes", 1})
		status = append(status, statusRow{"Innodb_log_waits", 0})
		status = append(status, statusRow{"Innodb_log_write_requests", 0})
		status = append(status, statusRow{"Innodb_log_writes", 2})
		status = append(status, statusRow{"Innodb_os_log_fsyncs", 4})
		status = append(status, statusRow{"Innodb_os_log_pending_fsyncs", 0})
		status = append(status, statusRow{"Innodb_os_log_pending_writes", 0})
		status = append(status, statusRow{"Innodb_os_log_written", 1024})
		status = append(status, statusRow{"Innodb_page_size", 16384})
		status = append(status, statusRow{"Innodb_pages_created", 35})
		status = append(status, statusRow{"Innodb_pages_read", 394})
		status = append(status, statusRow{"Innodb_pages_written", 39})
		status = append(status, statusRow{"Innodb_row_lock_current_waits", 0})
		status = append(status, statusRow{"Innodb_row_lock_time", 0})
		status = append(status, statusRow{"Innodb_row_lock_time_avg", 0})
		status = append(status, statusRow{"Innodb_row_lock_time_max", 0})
		status = append(status, statusRow{"Innodb_row_lock_waits", 0})
		status = append(status, statusRow{"Innodb_rows_deleted", 0})
		status = append(status, statusRow{"Innodb_rows_inserted", 13})
		status = append(status, statusRow{"Innodb_rows_read", 22})
		status = append(status, statusRow{"Innodb_rows_updated", 0})
		status = append(status, statusRow{"Innodb_num_open_files", 22})
		status = append(status, statusRow{"Innodb_truncated_status_writes", 0})
		status = append(status, statusRow{"Innodb_available_undo_logs", 128})
		status = append(status, statusRow{"Key_blocks_not_flushed", 0})
		status = append(status, statusRow{"Key_blocks_unused", 13394})
		status = append(status, statusRow{"Key_blocks_used", 3})
		status = append(status, statusRow{"Key_read_requests", 12})
		status = append(status, statusRow{"Key_reads", 5})
		status = append(status, statusRow{"Key_write_requests", 0})
		status = append(status, statusRow{"Key_writes", 0})
		status = append(status, statusRow{"Last_query_cost", 0})
		status = append(status, statusRow{"Last_query_partial_plans", 2})
		status = append(status, statusRow{"Locked_connects", 0})
		status = append(status, statusRow{"Max_execution_time_exceeded", 0})
		status = append(status, statusRow{"Max_execution_time_set", 0})
		status = append(status, statusRow{"Max_execution_time_set_failed", 0})
		status = append(status, statusRow{"Max_used_connections", 3})
		status = append(status, statusRow{"Max_used_connections_time", 0}) // TODO: 2019-03-05 08:22:32
		status = append(status, statusRow{"Not_flushed_delayed_rows", 0})
		status = append(status, statusRow{"Ongoing_anonymous_transaction_count", 0})
		status = append(status, statusRow{"Open_files", 8})
		status = append(status, statusRow{"Open_streams", 0})
		status = append(status, statusRow{"Open_table_definitions", 111})
		status = append(status, statusRow{"Open_tables", 50})
		status = append(status, statusRow{"Opened_files", 163})
		status = append(status, statusRow{"Opened_table_definitions", 0})
		status = append(status, statusRow{"Opened_tables", 1})
		status = append(status, statusRow{"Performance_schema_accounts_lost", 0})
		status = append(status, statusRow{"Performance_schema_cond_classes_lost", 0})
		status = append(status, statusRow{"Performance_schema_cond_instances_lost", 0})
		status = append(status, statusRow{"Performance_schema_digest_lost", 0})
		status = append(status, statusRow{"Performance_schema_file_classes_lost", 0})
		status = append(status, statusRow{"Performance_schema_file_handles_lost", 0})
		status = append(status, statusRow{"Performance_schema_file_instances_lost", 0})
		status = append(status, statusRow{"Performance_schema_hosts_lost", 0})
		status = append(status, statusRow{"Performance_schema_index_stat_lost", 0})
		status = append(status, statusRow{"Performance_schema_locker_lost", 0})
		status = append(status, statusRow{"Performance_schema_memory_classes_lost", 0})
		status = append(status, statusRow{"Performance_schema_metadata_lock_lost", 0})
		status = append(status, statusRow{"Performance_schema_mutex_classes_lost", 0})
		status = append(status, statusRow{"Performance_schema_mutex_instances_lost", 0})
		status = append(status, statusRow{"Performance_schema_nested_statement_lost", 0})
		status = append(status, statusRow{"Performance_schema_prepared_statements_lost", 0})
		status = append(status, statusRow{"Performance_schema_program_lost", 0})
		status = append(status, statusRow{"Performance_schema_rwlock_classes_lost", 0})
		status = append(status, statusRow{"Performance_schema_rwlock_instances_lost", 0})
		status = append(status, statusRow{"Performance_schema_session_connect_attrs_lost", 0})
		status = append(status, statusRow{"Performance_schema_socket_classes_lost", 0})
		status = append(status, statusRow{"Performance_schema_socket_instances_lost", 0})
		status = append(status, statusRow{"Performance_schema_stage_classes_lost", 0})
		status = append(status, statusRow{"Performance_schema_statement_classes_lost", 0})
		status = append(status, statusRow{"Performance_schema_table_handles_lost", 0})
		status = append(status, statusRow{"Performance_schema_table_instances_lost", 0})
		status = append(status, statusRow{"Performance_schema_table_lock_stat_lost", 0})
		status = append(status, statusRow{"Performance_schema_thread_classes_lost", 0})
		status = append(status, statusRow{"Performance_schema_thread_instances_lost", 0})
		status = append(status, statusRow{"Performance_schema_users_lost", 0})
		status = append(status, statusRow{"Prepared_stmt_count", 0})
		status = append(status, statusRow{"Qcache_free_blocks", 1})
		status = append(status, statusRow{"Qcache_free_memory", 16760152})
		status = append(status, statusRow{"Qcache_hits", 0})
		status = append(status, statusRow{"Qcache_inserts", 0})
		status = append(status, statusRow{"Qcache_lowmem_prunes", 0})
		status = append(status, statusRow{"Qcache_not_cached", 20})
		status = append(status, statusRow{"Qcache_queries_in_cache", 0})
		status = append(status, statusRow{"Qcache_total_blocks", 1})
		status = append(status, statusRow{"Queries", 107})
		status = append(status, statusRow{"Questions", 19})
		status = append(status, statusRow{"Select_full_join", 0})
		status = append(status, statusRow{"Select_full_range_join", 0})
		status = append(status, statusRow{"Select_range", 0})
		status = append(status, statusRow{"Select_range_check", 0})
		status = append(status, statusRow{"Select_scan", 12})
		status = append(status, statusRow{"Slave_open_temp_tables", 0})
		status = append(status, statusRow{"Slow_launch_threads", 0})
		status = append(status, statusRow{"Slow_queries", 0})
		status = append(status, statusRow{"Sort_merge_passes", 0})
		status = append(status, statusRow{"Sort_range", 0})
		status = append(status, statusRow{"Sort_rows", 0})
		status = append(status, statusRow{"Sort_scan", 0})
		status = append(status, statusRow{"Ssl_accept_renegotiates", 0})
		status = append(status, statusRow{"Ssl_accepts", 0})
		status = append(status, statusRow{"Ssl_callback_cache_hits", 0})
		status = append(status, statusRow{"Ssl_cipher", 0})      // TODO: nil
		status = append(status, statusRow{"Ssl_cipher_list", 0}) // TODO: nil
		status = append(status, statusRow{"Ssl_client_connects", 0})
		status = append(status, statusRow{"Ssl_connect_renegotiates", 0})
		status = append(status, statusRow{"Ssl_ctx_verify_depth", 0})
		status = append(status, statusRow{"Ssl_ctx_verify_mode", 0})
		status = append(status, statusRow{"Ssl_default_timeout", 0})
		status = append(status, statusRow{"Ssl_finished_accepts", 0})
		status = append(status, statusRow{"Ssl_finished_connects", 0})
		status = append(status, statusRow{"Ssl_server_not_after", 0})  // TODO: NONE
		status = append(status, statusRow{"Ssl_server_not_before", 0}) // TODO: NONE
		status = append(status, statusRow{"Ssl_session_cache_hits", 0})
		status = append(status, statusRow{"Ssl_session_cache_misses", 0})
		status = append(status, statusRow{"Ssl_session_cache_mode", 0}) // TODO: NONE
		status = append(status, statusRow{"Ssl_session_cache_overflows", 0})
		status = append(status, statusRow{"Ssl_session_cache_size", 0})
		status = append(status, statusRow{"Ssl_session_cache_timeouts", 0})
		status = append(status, statusRow{"Ssl_sessions_reused", 0})
		status = append(status, statusRow{"Ssl_used_session_cache_entries", 0})
		status = append(status, statusRow{"Ssl_verify_depth", 0})
		status = append(status, statusRow{"Ssl_verify_mode", 0})
		status = append(status, statusRow{"Ssl_version", 0}) // TODO: NONE
		status = append(status, statusRow{"Table_locks_immediate", 128})
		status = append(status, statusRow{"Table_locks_waited", 0})
		status = append(status, statusRow{"Table_open_cache_hits", 5})
		status = append(status, statusRow{"Table_open_cache_misses", 1})
		status = append(status, statusRow{"Table_open_cache_overflows", 0})
		status = append(status, statusRow{"Tc_log_max_pages_used", 0})
		status = append(status, statusRow{"Tc_log_page_size", 0})
		status = append(status, statusRow{"Tc_log_page_waits", 0})
		status = append(status, statusRow{"Threads_cached", 0})
		status = append(status, statusRow{"Threads_connected", 3})
		status = append(status, statusRow{"Threads_created", 3})
		status = append(status, statusRow{"Threads_running", 1})
		status = append(status, statusRow{"Uptime", 429493})
		status = append(status, statusRow{"Uptime_since_flush_status", 429493})
		status = append(status, statusRow{"validate_password_dictionary_file_last_parsed", 0}) // TODO: 2019-02-28 09:05:28
		status = append(status, statusRow{"validate_password_dictionary_file_words_count", 0})
	}

	var rows []sql.Row
	for _, row := range status {
		if filter == row.name || filter == filterNone {
			rows = append(rows, sql.NewRow([]byte(row.name), row.value))
		}
	}
	rowIter := sql.RowsToRowIter(rows...)

	return sql.Schema(cols), rowIter, nil
}

type statusRow struct {
	name  string
	value int64
}
