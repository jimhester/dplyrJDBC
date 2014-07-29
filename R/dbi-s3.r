#' @export
db_info <- function(con) UseMethod("db_info")
db_info.default <- function(con) dbGetInfo(con)
db_info.JDBCConnection <- function(con) {
# TODO
}

db_list_tables.JDBCConnection <- function(con) {
  table_fields <- dbGetTables(con)

  # some JDBC connectors use upper case for these results
  names(table_fields) <- tolower(names(table_fields))

  # ignore system tables
  table_fields <- table_fields[!(is.na(table_fields[['table_type']]) | grepl('SYSTEM', table_fields[['table_type']])), ]

  table_fields[["table_name"]]
  #paste(table_fields[["table_schem"]], table_fields[["table_name"]], sep = ".")
}

db_table_source <- function(con, path, from) UseMethod("db_table_source")

#' @export
db_table_source.default <- function(con, path, from) {
  if (!is.sql(from)) { # Must be a character string
    if (isFALSE(db_has_table(con, from))) {
      stop("Table ", from, " not found in database ", path, call. = FALSE)
    }

    from <- ident(from)
  } else if (!is.join(from)) { # Must be arbitrary sql
    # Abitrary sql needs to be wrapped into a named subquery
    from <- build_sql("(", from, ") AS ", ident(unique_name()), con = con)
  }
  from
}
#' @export
db_table_source.PostgreSQLConnection <- function(con, path, from) {
  if (!is.sql(from)) { # Must be a character string
    tblschema <- NA
    parts <- strsplit(from, "\\.")[[1]]
    if (length(parts) == 1) {
      tblname <- parts[[1]]
    }
    else if (length(parts)== 2) {
      tblschema <- parts[[1]]
      tblname <- parts[[2]]
    }
    else {
      stop("Invalid table format ", from, ". [schema.]tablename is accepted.", call. = FALSE)
    }

    if (isFALSE(db_has_table(con, tblname))) {
      stop("Table ", from, " not found in database ", path, call. = FALSE)
    }

    if (!is.na(tblschema)) {
      from <- build_sql(ident(tblschema), ".", ident(tblname))
    }
    else {
      from <- ident(tblname)
    }
  } else if (!is.join(from)) { # Must be arbitrary sql
    # Abitrary sql needs to be wrapped into a named subquery
    from <- build_sql("(", from, ") AS ", ident(unique_name()), con = con)
  }
  from
}

DbDisconnector <- setRefClass("DbDisconnector",
  fields = c("con", "name", "quiet"),
  methods = list(
    finalize = function() {
      if (!quiet) {
        message("Auto-disconnecting ", name, " connection ",
          "(", paste(get_slot(con, 'Id'), collapse = ", "), ")")
      }
      dbDisconnect(con)
    }
  )
)

#' @export
qry_fields.JDBCConnection <- function(con, from) {
  qry <- dbSendQuery(con, build_sql("SELECT * FROM ", from, " WHERE 0=1;"))
  on.exit(dbClearResult(qry))

  info = dbColumnInfo(qry)
}

table_fields <- function(con, table) UseMethod("table_fields")
#' @export
table_fields.DBIConnection <- function(con, table) dbListFields(con, table)

#' @export
table_fields.PostgreSQLConnection <- function(con, table) {
  qry_fields.DBIConnection(con, table)
}
#' @export
table_fields.JDBCConnection <- function(con, table) dbListFields(con, table)


#' @export
table_fields.bigquery <- function(con, table) {
  info <- get_table(con$project, con$dataset, table)
  vapply(info$schema$fields, "[[", "name", FUN.VALUE = character(1))
}

# Run a query, abandoning results
table_fields <- function(con, table) UseMethod("table_fields")
qry_run <- function(con, sql, data = NULL, in_transaction = FALSE,
                    show = getOption("dplyr.show_sql"),
                    explain = getOption("dplyr.explain_sql")) {
  UseMethod("qry_run")
}

qry_run.default <- function(con, sql, data = NULL, in_transaction = FALSE,
                                   show = getOption("dplyr.show_sql"),
                                   explain = getOption("dplyr.explain_sql")) {
  if (show) message(sql)
  if (explain) message(qry_explain(con, sql))

  if (in_transaction) {
    dbBeginTransaction(con)
    on.exit(dbCommit(con))
  }

  if (is.null(data)) {
    res <- dbSendQuery(con, sql)
  } else {
    res <- dbSendPreparedQuery(con, sql, bind.data = data)
  }
  dbClearResult(res)

  invisible(NULL)
}

qry_run.JDBCConnection <- function(con, sql, data = NULL, in_transaction = FALSE,
                                   show = getOption("dplyr.show_sql"),
                                   explain = getOption("dplyr.explain_sql")) {
  if (show) message(sql)
  if (explain) message(qry_explain(con, sql))

  if (in_transaction) {
    dbBeginTransaction(con)
    on.exit(dbCommit(con))
  }

  if (is.null(data)) {
    dbSendUpdate(con, sql)
    #dbCommit(con)
  } else {
    res <- dbSendPreparedQuery(con, sql, bind.data = data)
    dbClearResult(res)
  }

  invisible(NULL)
}

#' @export
sql_insert_into.JDBCConnection <- function(con, table, values) {
  cols <- lapply(values, escape, collapse = NULL, parens = FALSE, con = con)
  col_mat <- matrix(unlist(cols, use.names = FALSE), nrow = nrow(values))

  rows <- apply(col_mat, 1, paste0, collapse = ", ")
  values <- paste0("(", rows, ")", collapse = "\n, ")

  sql <- build_sql("INSERT INTO ", ident(table), " VALUES ", sql(values))
  qry_run(con, sql)
}
