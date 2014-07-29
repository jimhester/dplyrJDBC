#' @export
lahman_JDBC <- function(...) {
  cache_JDBC_postgres(...)
}


cache_JDBC_postgres = function(...) {
  cache_name <- "lahman_JDBC_postgres"
  if (dplyr:::is_cached(cache_name)) return(get_cache(cache_name))

  src <- src_JDBC(JDBC("org.postgresql.Driver",
                       "postgresql-9.3.1101.jdbc41.jar"),
                  "jdbc:postgresql://localhost/lahman_jdbc", identifier.quote="'", ...)

  tables <- setdiff(dplyr:::lahman_tables(), src_tbls(src))

  # Create missing tables
  for(table in tables) {
    df <- getExportedValue("Lahman", table)
    message("Creating table: ", table)

    ids <- as.list(names(df)[grepl("ID$", names(df))])
    copy_to(src, df, table, indexes = ids, temporary = FALSE)
  }

  dplyr:::set_cache(cache_name, src)
}
