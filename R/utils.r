get_slot <- function(x, name, default='') {
  if(name %in% slotNames(x)) {
    slot(x, name)
  }
  else {
    default
  }
}
