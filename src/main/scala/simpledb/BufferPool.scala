package simpledb

/**
  * BufferPool manages the reading and writing of pages into memory from
  * disk. Access methods call into it to retrieve pages, and it fetches
  * pages from the appropriate location.
  * <p>
  * The BufferPool is also responsible for locking;  when a transaction fetches
  * a page, BufferPool which check that the transaction has the appropriate
  * locks to read/write the page.
  *
  * @constructor Creates a BufferPool that caches up to numPages pages.
  * @param numPages Maximum number of pages in this buffer pool.
  */
class BufferPool(numPages: Int) {
  // TODO
  ???

  /**
    * Retrieve the specified page with the associated permissions.
    * Will acquire a lock and may block if that lock is held by another
    * transaction.
    * <p>
    * The retrieved page should be looked up in the buffer pool.  If it
    * is present, it should be returned.  If it is not present, it should
    * be added to the buffer pool and returned.  If there is insufficient
    * space in the buffer pool, an page should be evicted and the new page
    * should be added in its place.
    *
    * @param tid the ID of the transaction requesting the page
    * @param pid the ID of the requested page
    * @param perm the requested permissions on the page
    */
  def getPage(tid: TransactionId, pid: PageId, perm: Permissions): Page = ???

  /**
    * Releases the lock on a page.
    * Calling this is very risky, and may result in wrong behavior. Think hard
    * about who needs to call this and why, and why they can run the risk of
    * calling it.
    *
    * @param tid the ID of the transaction requesting the unlock
    * @param pid the ID of the page to unlock
    */
  def releasePage(tid: TransactionId, pid: PageId): Unit = ???

  /**
    * Release all locks associated with a given transaction.
    *
    * @param tid the ID of the transaction requesting the unlock
    */
  def transactionComplete(tid: TransactionId): Unit = ???

  /** Return true if the specified transaction has a lock on the specified page */
  def holdsLock(tid: TransactionId, p: PageId): Boolean = ???

  /**
    * Commit or abort a given transaction; release all locks associated to
    * the transaction.
    *
    * @param tid the ID of the transaction requesting the unlock
    * @param commit a flag indicating whether we should commit or abort
    */
  def transactionComplete(tid: TransactionId, commit: Boolean): Unit = ???

  /**
    * Add a tuple to the specified table behalf of transaction tid.  Will
    * acquire a write lock on the page the tuple is added to(Lock
    * acquisition is not needed for lab2). May block if the lock cannot
    * be acquired.
    *
    * Marks any pages that were dirtied by the operation as dirty by calling
    * their markDirty bit, and updates cached versions of any pages that have
    * been dirtied so that future requests see up-to-date pages.
    *
    * @param tid the transaction adding the tuple
    * @param tableId the table to add the tuple to
    * @param t the tuple to add
    */
  def insertTuple(tid: TransactionId, tableId: Int, t: Tuple): Unit = ???

  /**
    * Remove the specified tuple from the buffer pool.
    * Will acquire a write lock on the page the tuple is removed from. May block if
    * the lock cannot be acquired.
    *
    * Marks any pages that were dirtied by the operation as dirty by calling
    * their markDirty bit.  Does not need to update cached versions of any pages that have
    * been dirtied, as it is not possible that a new page was created during the deletion
    * (note difference from addTuple).
    *
    * @param tid the transaction adding the tuple.
    * @param t the tuple to add
    */
  def deleteTuple(tid: TransactionId, t: Tuple): Unit = ???

  /**
    * Flush all dirty pages to disk.
    * NB: Be careful using this routine -- it writes dirty data to disk so will
    *     break simpledb if running in NO STEAL mode.
    */
  def flushAllPages(): Unit = this.synchronized {
    ???
  }

  /** Remove the specific page id from the buffer pool.
    * Needed by the recovery manager to ensure that the
    * buffer pool doesn't keep a rolled back page in its
    * cache.
    */
  def discardPage(pid: PageId): Unit = this.synchronized {
    ???
  }

  /**
    * Flushes a certain page to disk
    * @param pid an ID indicating the page to flush
    */
  private def flushPage(pid: PageId): Unit = this.synchronized {
    ???
  }

  /** Write all pages of the specified transaction to disk.
    */
  def flushPages(tid: TransactionId): Unit = this.synchronized {
    ???
  }

  /**
    * Discards a page from the buffer pool.
    * Flushes the page to disk to ensure dirty pages are updated on disk.
    */
  private def evictPage(): Unit = this.synchronized {
    ???
  }
}

object BufferPool {
  /** Bytes per page, including header. */
  final val PAGE_SIZE: Int = 4096

  /** Default number of pages passed to the constructor.
    * This is used by other classes. BufferPool should use the numPages argument to the
    * constructor instead.
    */
  final val DEFAULT_PAGES: Int = 50
}

