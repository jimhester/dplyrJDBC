get_slot <- function(x, name, default='') {
  if(name %in% slotNames(x)) {
    slot(x, name)
  }
  else {
    default
  }
}

"%||%" <- function(x, y) if(is.null(x)) y else x
