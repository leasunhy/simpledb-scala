package simpledb

import java.util._

/**
  * DbFileIterator is the iterator interface that all SimpleDB Dbfile should implement.
  */
trait DbFileIterator {
  /**
    * Opens the iterator
    * @throws DbException when there are problems opening/accessing the database.
    */
  def open(): Unit

  /** @return true if there are more tuples available. */
  def hasNext(): Boolean

  /**
    * Gets the next tuple from the operator (typically implementing by reading
    * from a child operator or an access method).
    *
    * @return The next tuple in the iterator.
    * @throws NoSuchElementException if there are no more tuples
    */
  def next(): Tuple

  /**
    * Resets the iterator to the start.
    * @throws DbException When rewind is unsupported.
    */
  def rewind(): Unit

  /**
    * Closes the iterator.
    */
  def close(): Unit
}
