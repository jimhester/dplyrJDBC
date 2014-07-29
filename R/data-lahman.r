lahman_src <- function(type, ...) {
  switch(type,
    df = lahman_df(),
    dt = lahman_dt(),
    sqlite = src_sqlite(db_location(filename = "lahman.sqlite"), ...),
    mysql = src_mysql("lahman", ...),
    monetdb = src_monetdb("lahman", ...),
    postgres = src_postgres("lahman", ...),
    JDBC_postgres = src_JDBC(JDBC("org.postgresql.Driver",
                                  "$PWD/postgresql-9.3.1101.jdbc41.jar"),
                             "jdbc:postgresql://localhost/lahman_jdbc", identifier.quote="'", ...),
    bigquery = src_bigquery(Sys.getenv("BIGQUERY_PROJECT"), "lahman", ...),
    stop("Unknown src type ", type, call. = FALSE)
  )
}
