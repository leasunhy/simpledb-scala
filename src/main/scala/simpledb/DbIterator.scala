package simpledb

/**
  * DbIterator is the iterator interface that all SimpleDB operators should
  * implement.
  * If the iterator is not open, none of the methods should work,
  * and should throw an IllegalStateException.  In addition to any
  * resource allocation/deallocation, an open method should call any
  * child iterator open methods, and in a close method, an iterator
  * should call its children's close methods.
  */
trait DbIterator extends Iterator[Tuple] {
  /**
    * Opens the iterator
    * @throws DbException when there are problems opening/accessing the database.
    */
  def open(): Unit

  /** @return true if there are more tuples available.
    * @throws IllegalStateException If the iterator has not been opened
    */
  def hasNext(): Boolean

  /**
    * Gets the next tuple from the operator (typically implementing by reading
    * from a child operator or an access method).
    *
    * @return The next tuple in the iterator.
    * @throws NoSuchElementException if there are no more tuples
    * @throws IllegalStateException If the iterator has not been opened
    */
  def next(): Tuple

  /**
    * Resets the iterator to the start.
    * @throws DbException When rewind is unsupported.
    * @throws IllegalStateException If the iterator has not been opened
    */
  def rewind(): Unit

  /**
    * Returns the TupleDesc associated with this DbIterator.
    * @return the TupleDesc associated with this DbIterator.
    */
  def getTupleDesc: TupleDesc

  /**
    * Closes the iterator.
    * When the iterator is closed, calling next(), hasNext(), or rewind() should fail by throwing IllegalStateException.
    */
  def close(): Unit
}
