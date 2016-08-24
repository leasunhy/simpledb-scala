package simpledb

/**
  * A RecordId is a reference to a specific tuple on a specific page of a
  * specific table.
  *
  * @constructor Creates a new RecordId refering to the specified PageId and tuple number.
  * @param pid the pageid of the page on which the tuple resides
  * @param tupleno the tuple number within the page.
  */
class RecordId(pid: PageId, tupleno: Int) {
  // TODO
  ???

  /**
    * @return the tuple number this RecordId references.
    */
  def getTupleNo: Int = ???

  /**
    * @return the page id this RecordId references.
    */
  def getPageId: PageId = ???

  /**
    * Two RecordId objects are considered equal if they represent the same tuple.
    *
    * @return True if this and o represent the same tuple
    */
  override def equals(obj: scala.Any): Boolean = ???

  /**
    * You should implement the hashCode() so that two equal RecordId instances
    * (with respect to equals()) have the same hashCode().
    *
    * @return An int that is the same for equal RecordId objects.
    */
  override def hashCode(): Int = ???
}
