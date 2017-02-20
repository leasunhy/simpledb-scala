package simpledb

/**
  * A RecordId is a reference to a specific tuple on a specific page of a
  * specific table.
  *
  * @constructor Creates a new RecordId referring to the specified PageId and tuple number.
  * @param pid the pageid of the page on which the tuple resides
  * @param tupleno the tuple number within the page.
  */
class RecordId(pid: PageId, tupleno: Int) {
  /**
    * @return the tuple number this RecordId references.
    */
  def getTupleNo: Int = tupleno

  /**
    * @return the page id this RecordId references.
    */
  def getPageId: PageId = pid

  /**
    * Two RecordId objects are considered equal if they represent the same tuple.
    *
    * @return True if this and o represent the same tuple
    */
  override def equals(obj: scala.Any): Boolean = obj match {
    case rid: RecordId => pid == rid.getPageId && tupleno == rid.getTupleNo
    case _ => false
  }

  /**
    * You should implement the hashCode() so that two equal RecordId instances
    * (with respect to equals()) have the same hashCode().
    *
    * @return An int that is the same for equal RecordId objects.
    */
  override def hashCode(): Int = pid.hashCode() * 11 + tupleno
}
