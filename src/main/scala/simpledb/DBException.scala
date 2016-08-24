package simpledb

class DBException(s: String) extends Exception(s) {
}

object DBException {
  final val serialVersionUID: Long = 1l
}
