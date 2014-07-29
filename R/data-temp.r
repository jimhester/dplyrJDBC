temp_src <- function(type, ...) {
  cache_name <- paste("temp", type, "src", collapse = "-")
  if (is_cached(cache_name)) return(get_cache(cache_name))

  env <- new.env(parent = emptyenv())
  src <- switch(type,
    df =       src_df(env = env),
    dt =       src_dt(env = env),
    sqlite =   src_sqlite(tempfile(), create = TRUE),
    mysql =    src_mysql("test", ...),
    postgres = src_postgres("test", ...),
    bigquery = src_bigquery(Sys.getenv("BIGQUERY_PROJECT"), "test", ...),
    JDBC_postgres = src_JDBC(JDBC("org.postgresql.Driver",
                                  "postgresql-9.3.1101.jdbc41.jar"),
                             "jdbc:postgresql://localhost/test_jdbc", identifier.quote='"', ...),
    stop("Unknown src type ", type, call. = FALSE)
  )

  set_cache(cache_name, src)
}
