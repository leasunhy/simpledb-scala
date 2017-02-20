package simpledb

import java.io._
import java.nio.ByteBuffer
import java.nio.channels.FileChannel

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
  private val fileChannel = new FileInputStream(f).getChannel
  private val fileSize = fileChannel.size()
  private val buffer = ByteBuffer.allocate(BufferPool.PAGE_SIZE)

  /**
    * Returns the File backing this HeapFile on disk.
    *
    * @return the File backing this HeapFile on disk.
    */
  def getFile: File = f

  /**
    * Returns an ID uniquely identifying this HeapFile. Implementation note:
    * you will need to generate this tableid somewhere ensure that each
    * HeapFile has a "unique id," and that you always return the same value
    * for a particular HeapFile. We suggest hashing the absolute file name of
    * the file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
    *
    * @return an ID uniquely identifying this HeapFile.
    */
  def getId: Int = f.getAbsoluteFile.hashCode()

  /**
    * Returns the TupleDesc of the table stored in this DbFile.
    * @return TupleDesc of this DbFile.
    */
  def getTupleDesc: TupleDesc = td

  /**
    * Returns the number of pages in this HeapFile.
    */
  def numPages: Int = (fileSize / BufferPool.PAGE_SIZE).toInt

  /**
    * Read the specified page from disk.
    *
    * @throws IllegalArgumentException if the page does not exist in this file.
    */
  override def readPage(id: PageId): Page = {
    val offset = id.getPageNo * BufferPool.PAGE_SIZE
    fileChannel.position(offset)
    buffer.position(0)
    fileChannel.read(buffer)
    new HeapPage(new HeapPageId(id.getTableId, id.getPageNo), buffer.array().clone())
  }

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
  override def deleteTuple(tid: TransactionId, t: Tuple) : Page = ???

  /**
    * Returns an iterator over all the tuples stored in this DbFile.
    * The iterator must use {@link BufferPool#getPage}, rather than
    * {@link #readPage} to iterator through the pages.
    *
    * @return an iterator over all the tuples stored in this DbFile.
    */
  override def iterator(tid: TransactionId): DbFileIterator = new DbFileIterator {
    private var currentPageIterator: Iterator[Tuple] = _
    private var pageIndex = -1

    override def next(): Tuple = {
      if (pageIndex == -1)
        throw new NoSuchElementException("Iterator not opened.")
      while (pageIndex < numPages - 1 && !currentPageIterator.hasNext) {
        pageIndex += 1
        changeIterator()
      }
      if (pageIndex > numPages - 1)
        throw new NoSuchElementException("Iterator exhausted.")
      currentPageIterator.next()
    }

    override def hasNext(): Boolean = pageIndex != -1 && (currentPageIterator.hasNext || pageIndex < numPages - 1)

    override def close(): Unit = {
      currentPageIterator = null
      pageIndex = -1
    }

    override def rewind(): Unit = {
      if (pageIndex == -1)
        throw new DbException("Iterator not opened.")
      pageIndex = 0
      changeIterator()
    }

    override def open(): Unit = {
      if (pageIndex != -1)
        throw new DbException("Iterator already opened.")
      if (numPages == 0) throw new DbException("The file is empty.")
      pageIndex = 0
      changeIterator()
    }

    private def changeIterator(): Unit = {
      val page = Database.getBufferPool.getPage(tid, new HeapPageId(getId, pageIndex), Permissions.READ_ONLY)
      currentPageIterator = page.asInstanceOf[HeapPage].iterator
    }
  }
}
