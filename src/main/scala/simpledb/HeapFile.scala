package simpledb

import java.io._

/**
  * HeapFile is an implementation of a DbFile that stores a collection
  * of tuples in no particular order.  Tuples are stored on pages, each of
  * which is a fixed size, and the file is simply a collection of those
  * pages. HeapFile works closely with HeapPage.  The format of HeapPages
  * is described in the HeapPage constructor.
  *
  * @see simpledb.HeapPage#HeapPage
  * @author Sam Madden (original Java version)
  *
  * @constructor Constructs a heap file backed by the specified file.
  */
class HeapFile(f: File, td: TupleDesc) extends DbFile {
  // TODO
  ???

  /**
    * Returns the File backing this HeapFile on disk.
    *
    * @return the File backing this HeapFile on disk.
    */
  def getFile: File = ???

  /**
    * Returns an ID uniquely identifying this HeapFile. Implementation note:
    * you will need to generate this tableid somewhere ensure that each
    * HeapFile has a "unique id," and that you always return the same value
    * for a particular HeapFile. We suggest hashing the absolute file name of
    * the file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
    *
    * @return an ID uniquely identifying this HeapFile.
    */
  def getId: Int = ???

  /**
    * Returns the TupleDesc of the table stored in this DbFile.
    * @return TupleDesc of this DbFile.
    */
  def getTupleDesc: TupleDesc = ???

  /**
    * Returns the number of pages in this HeapFile.
    */
  def numPages: Int = ???

  /**
    * Read the specified page from disk.
    *
    * @throws IllegalArgumentException if the page does not exist in this file.
    */
  override def readPage(id: PageId): Page = ???

  /**
    * Push the specified page to disk.
    *
    * @param p The page to write.  page.getId().pageno() specifies the offset into the file where the page should be written.
    * @throws IOException if the write fails
    *
    */
  override def writePage(p: Page): Unit = ???

  /**
    * Adds the specified tuple to the file on behalf of transaction.
    * This method will acquire a lock on the affected pages of the file, and
    * may block until the lock can be acquired.
    *
    * @param tid The transaction performing the update
    * @param t   The tuple to add.  This tuple should be updated to reflect that
    *            it is now stored in this file.
    * @return An ArrayList contain the pages that were modified
    * @throws DbException if the tuple cannot be added
    * @throws IOException if the needed file can't be read/written
    */
  override def addTuple(tid: TransactionId, t: Tuple): Vector[Page] = ???

  /**
    * Removes the specifed tuple from the file on behalf of the specified
    * transaction.
    * This method will acquire a lock on the affected pages of the file, and
    * may block until the lock can be acquired.
    *
    * @throws DbException if the tuple cannot be deleted or is not a member
    *                     of the file
    */
  override def iterator(tid: TransactionId): DbFileIterator = ???
}
