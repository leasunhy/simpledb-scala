package simpledb

/**
  * Implements a DbIterator by wrapping an Iterable<Tuple>.
  *
  * @constructor Constructs an iterator from the specified Iterable, and the specified descriptor.
  * @param tuples The set of tuples to iterate over
  */
class TupleIterator(td: TupleDesc, tuples: Iterable[Tuple]) extends DbIterator {
  var i: Iterator[Tuple] = _

  if (tuples.exists(!_.getTupleDesc().equals(td)))
    throw new IllegalArgumentException("Incompatible tuple in tuple set")

  /**
    * Opens the iterator
    *
    * @throws DbException when there are problems opening/accessing the database.
    */
  override def open(): Unit = i = tuples.iterator

  /** @return true if there are more tuples available.
    * @throws IllegalStateException If the iterator has not been opened
    */
  override def hasNext(): Boolean = i.hasNext

  /**
    * Gets the next tuple from the operator (typically implementing by reading
    * from a child operator or an access method).
    *
    * @return The next tuple in the iterator.
    * @throws NoSuchElementException if there are no more tuples
    * @throws IllegalStateException  If the iterator has not been opened
    */
  override def next(): Tuple = i.next()

  /**
    * Resets the iterator to the start.
    *
    * @throws DbException           When rewind is unsupported.
    * @throws IllegalStateException If the iterator has not been opened
    */
  override def rewind(): Unit = {
    close()
    open()
  }

  /**
    * Returns the TupleDesc associated with this DbIterator.
    *
    * @return the TupleDesc associated with this DbIterator.
    */
  override def getTupleDesc: TupleDesc = td

  /**
    * Closes the iterator.
    * When the iterator is closed, calling next(), hasNext(), or rewind() should fail by throwing IllegalStateException.
    */
  override def close(): Unit = i = null
}
