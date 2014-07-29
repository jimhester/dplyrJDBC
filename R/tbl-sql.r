#' @rdname dplyr::tbl_sql
#' @export
tbl_sql <- function(subclass, src, from, ..., vars = NULL) {
  assert_that(is.character(from), length(from) == 1)

  from <- db_table_source(src$con, src$path, from)

  tbl <- make_tbl(c(subclass, "sql"),
    src = src,              # src object
    from = from,            # table, join, or raw sql
    select = vars,          # SELECT: list of symbols
    summarise = FALSE,      #   interpret select as aggreagte functions?
    mutate = FALSE,         #   do select vars include new variables?
    where = NULL,           # WHERE: list of calls
    group_by = NULL,        # GROUP_BY: list of names
    order_by = NULL         # ORDER_BY: list of calls
  )
  update(tbl)
}
