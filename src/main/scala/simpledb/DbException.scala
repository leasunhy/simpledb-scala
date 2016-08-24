package simpledb

class DbException(s: String) extends Exception(s) {
}

object DbException {
  final val serialVersionUID: Long = 1l
}
