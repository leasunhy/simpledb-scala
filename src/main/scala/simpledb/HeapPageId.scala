package simpledb

/**
  * Unique identifier for HeapPage objects.
  *
  * @constructor Constructor. Create a page id structure for a specific page of a specific table.
  *
  * @param tableId The table that is being referenced
  * @param pgNo The page number in that table.
  */
class HeapPageId(tableId: Int, pgNo: Int) extends PageId {
  /** @return the unique tableid hashcode with this PageId */
  override def getTableId: Int = tableId

  override def getPageNo: Int = pgNo

  /**
    * @return a hash code for this page, represented by the concatenation of
    *   the table number and the page number (needed if a PageId is used as a
    *   key in a hash table in the BufferPool, for example.)
    * @see BufferPool
    */
  override def hashCode(): Int = tableId * 104729 + getPageNo * 48611

  /**
    * Compares one PageId to another.
    *
    * @param o The object to compare against (must be a PageId)
    * @return true if the objects are equal (e.g., page numbers and table ids are the same)
    */
  override def equals(o: Any): Boolean = o match {
    case null => false
    case other: PageId => this.tableId == other.getTableId && this.pgNo == other.getPageNo
    case _ => false
  }

  /**
    * Return a representation of this page id object as a collection of
    * integers (used for logging)
    *
    * Size of returned array must contain number of integers that corresponds to
    * number of args to one of the constructors.
    */
  override def serialize(): Array[Int] = Array(getTableId, getPageNo)
}
