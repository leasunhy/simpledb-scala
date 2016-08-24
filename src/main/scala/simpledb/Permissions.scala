package simpledb

/** Class representing requested permissions to a relation/title.
  * Private constructor with two static objects READ_ONLY and READ_WRITE that
  * represent the two levels of permission.
  */
class Permissions private (val permLevel: Int) {
  override def toString: String = permLevel match {
    case 0 => "READ_ONLY"
    case 1 => "READ_WRITE"
    case _ => "UNKNOWN"
  }
}

object Permissions {
  val READ_ONLY  = new Permissions(0)
  val READ_WRITE = new Permissions(1)
}
