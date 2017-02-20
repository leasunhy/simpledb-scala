package simpledb

/**
  * SeqScan is an implementation of a sequential scan access method that reads
  * each tuple of a table in no particular order (e.g., as they are laid out on
  * disk).
  *
  * @constructor Creates a sequential scan over the specified table as a part of the
  *              specified transaction.
  * @param tid The transaction this scan is running as a part of.
  * @param tableid the table to scan.
  * @param tableAlias the alias of this table (needed by the parser);
  *         the returned tupleDesc should have fields with name tableAlias.fieldName
  *         (note: this class is not responsible for handling a case where tableAlias
  *         or fieldName are null.  It shouldn't crash if they are, but the resulting
  *         name can be null.fieldName, tableAlias.null, or null.null).
  */
class SeqScan(tid: TransactionId, tableid: Int, tableAlias: String) extends DbIterator {
  private var iterator: DbFileIterator = _

  private def withIteratorCheck[A](f: () => A): A = {
    if (iterator == null)
      throw new IllegalStateException("The iterator is not opened.")
    f()
  }

  /**
    * Opens the iterator
    *
    * @throws DbException when there are problems opening/accessing the database.
    */
  override def open(): Unit = {
    iterator = Database.getCatalog.getDbFile(tableid).iterator(tid)
    iterator.open()
  }

  /** @return true if there are more tuples available.
    * @throws IllegalStateException If the iterator has not been opened
    */
  override def hasNext(): Boolean = withIteratorCheck{() => iterator.hasNext()}

  /**
    * Gets the next tuple from the operator (typically implementing by reading
    * from a child operator or an access method).
    *
    * @return The next tuple in the iterator.
    * @throws NoSuchElementException if there are no more tuples
    * @throws IllegalStateException  If the iterator has not been opened
    */
  override def next(): Tuple = withIteratorCheck{() => iterator.next()}

  /**
    * Resets the iterator to the start.
    *
    * @throws DbException           When rewind is unsupported.
    * @throws IllegalStateException If the iterator has not been opened
    */
  override def rewind(): Unit = withIteratorCheck{() => iterator.rewind()}

  /**
    * Returns the TupleDesc with field names from the underlying HeapFile,
    * prefixed with the tableAlias string from the constructor.
    * @return the TupleDesc with field names from the underlying HeapFile,
    * prefixed with the tableAlias string from the constructor.
    */
  override def getTupleDesc: TupleDesc = Database.getCatalog.getTupleDesc(tableid)

  /**
    * Closes the iterator.
    * When the iterator is closed, calling next(), hasNext(), or rewind() should fail by throwing IllegalStateException.
    */
  override def close(): Unit = iterator = null
}
