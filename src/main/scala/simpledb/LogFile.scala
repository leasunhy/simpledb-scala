package simpledb

import java.io.{EOFException, File, IOException, RandomAccessFile}
import java.lang.reflect.InvocationTargetException

/**
LogFile implements the recovery subsystem of SimpleDb.  This class is
able to write different log records as needed, but it is the
responsibility of the caller to ensure that write ahead logging and
two-phase locking discipline are followed.  <p>

<u> Locking note: </u>
<p>

Many of the methods here are synchronized (to prevent concurrent log
writes from happening); many of the methods in BufferPool are also
synchronized (for similar reasons.)  Problem is that BufferPool writes
log records (on page flushed) and the log file flushes BufferPool
pages (on checkpoints and recovery.)  This can lead to deadlock.  For
that reason, any LogFile operation that needs to access the BufferPool
must not be declared synchronized and must begin with a block like:

<p>
<pre>
    synchronized (Database.getBufferPool()) {
       synchronized (this) {

       ..

       }
    }
</pre>
*/

/**
<p> The format of the log file is as follows:

<ul>

<li> The first long integer of the file represents the offset of the
last written checkpoint, or -1 if there are no checkpoints

<li> All additional data in the log consists of log records.  Log
records are variable length.

<li> Each log record begins with an integer type and a long integer
transaction id.

<li> Each log record ends with a long integer file offset representing
the position in the log file where the record began.

<li> There are five record types: ABORT, COMMIT, UPDATE, BEGIN, and
CHECKPOINT

<li> ABORT, COMMIT, and BEGIN records contain no additional data

<li>UPDATE RECORDS consist of two entries, a before image and an
after image.  These images are serialized Page objects, and can be
accessed with the LogFile.readPageData() and LogFile.writePageData()
methods.  See LogFile.print() for an example.

<li> CHECKPOINT records consist of active transactions at the time
the checkpoint was taken and their first log record on disk.  The format
of the record is an integer count of the number of transactions, as well
as a long integer transaction id and a long integer first record offset
for each active transaction.

</ul>

  @constructor Initialize and back the log file with the specified file.
               We're not sure yet whether the caller is creating a brand new DB,
               in which case we should ignore the log file, or whether the caller
               will eventually want to recover (after populating the Catalog).
               So we make this decision lazily: if someone calls recover(), then
               do it, while if someone starts adding log file entries, then first
               throw out the initial log file contents.
  @param logFile The log file's name
*/
class LogFile(logFile: File) {
  var raf = new RandomAccessFile(logFile, "rw")
  var recoveryUndecided = true

  // install shutdown hook to force cleanup on close
  // Runtime.getRuntime().addShutdownHook(new Thread() {
  // public void run() { shutdown(); }
  // });

  //XXX WARNING -- there is nothing that verifies that the specified
  // log file actually corresponds to the current catalog.
  // This could cause problems since we log tableids, which may or
  // may not match tableids in the current catalog.

  var currentOffset: Long = -1
  var pageSize: Int = 0
  var totalRecords: Int = 0

  val tidToFirstLogRecord = scala.collection.mutable.HashMap.empty[Long, Long]

  // we're about to append a log record. if we weren't sure whether the
  // DB wants to do recovery, we're sure now -- it didn't. So truncate
  // the log.
  def preAppend(): Unit = {
    totalRecords += 1
    if (recoveryUndecided) {
      recoveryUndecided = false
      raf.seek(0)
      raf.setLength(0)
      raf.writeLong(LogFile.NO_CHECKPOINT_ID)
      raf.seek(raf.length)
      currentOffset = raf.getFilePointer
    }
  }

  def getTotalRecords: Int = totalRecords

  /** Write an abort record to the log for the specified tid, force
    * the log to disk, and perform a rollback
    * @param tid The aborting transaction.
    */
  def logAbort(tid: TransactionId): Unit = Database.getBufferPool.syncronized {
    this.synchronized {
      preAppend()
      // Debug.log("ABORT")

      // should we verify that this is a live transaction
      // must do this here, since rollback only works for live transactions (needs tidToFirstLogRecord)
      rollback(tid)

      raf.writeInt(LogFile.ABORT_RECORD)
      raf.writeLong(tid.id)
      raf.writeLong(currentOffset)
      currentOffset = raf.getFilePointer
      force()
      tidToFirstLogRecord.remove(tid.id)
    }
  }

  /** Write a commit record to disk for the specified tid,
        and force the log to disk.

        @param tid The committing transaction.
    */
  def logCommit(tid: TransactionId): Unit = this.synchronized {
    preAppend()
    Debug.log("COMMIT " + tid.id)
    // should we verify that this is a live transaction

    raf.writeInt(LogFile.COMMIT_RECORD)
    raf.writeLong(tid.id)
    raf.writeLong(currentOffset)
    currentOffset = raf.getFilePointer
    force()
    tidToFirstLogRecord.remove(tid.id)
  }

  /** Write an UPDATE record to disk for the specified tid and page
    * (with provided before and after images.)
    * @param tid The transaction performing the write
    * @param before The before image of the page
    * @param after The after image of the page
    *
    * @see simpledb.Page#getBeforeImage
    */
  def LogWrite(tid: TransactionId, before: Page, after: Page): Unit = this.synchronized {
    Debug.log("WRITE, offset = " + raf.getFilePointer)
    preAppend()
    /* update record conists of

       record type
       transaction id
       before page data (see writePageData)
       after page data
       start offset
    */
    raf.writeInt(LogFile.UPDATE_RECORD)
    raf.writeLong(tid.id)

    writePageData(raf, before)
    writePageData(raf, after)

    raf.writeLong(currentOffset)
    currentOffset = raf.getFilePointer
    Debug.log("WRITE OFFSET = " + currentOffset)
  }

  def writePageData(raf: RandomAccessFile, p: Page): Unit = {
    val pid = p.getId()
    val pageInfo = pid.serialize()

    //page data is:
    // page class name
    // id class name
    // id class bytes
    // id class data
    // page class bytes
    // page class data

    val pageClassName = p.getClass.getName
    val idClassName = pid.getClass.getName

    raf.writeUTF(pageClassName)
    raf.writeUTF(idClassName)

    raf.writeInt(pageInfo.length)
    pageInfo.foreach(raf.writeInt)
    val pageData = p.getPageData()
    raf.writeInt(pageData.length)
    raf.write(pageData)
  }

  def readPageData(raf: RandomAccessFile): Page = {
    val pageClassName = raf.readUTF()
    val idClassName = raf.readUTF()

    try {
      val idClass = Class.forName(idClassName)
      val pageClass = Class.forName(pageClassName)

      val idConsts = idClass.getDeclaredConstructors
      val numIdArgs = raf.readInt()
      val idArgs = Array.fill[Object](numIdArgs, new Integer(raf.readInt))
      val pid = idConsts(0).newInstance(idArgs).asInstanceOf[PageId]

      val pageConsts = pageClass.getDeclaredConstructors
      val pageSize = raf.readInt()
      val pageData = Array.ofDim[Byte](pageSize)
      raf.read(pageData)
      val pageArgs = Array.ofDim[Object](2)
      pageArgs(0) = pid
      pageArgs(1) = pageData

      pageConsts(0).newInstance(pageArgs).asInstanceOf[Page]
    } catch {
      case e: ClassNotFoundException =>
        e.printStackTrace()
        throw new IOException()
      case e: InstantiationException =>
        e.printStackTrace()
        throw new IOException()
      case e: IllegalAccessException =>
        e.printStackTrace()
        throw new IOException()
      case e: InvocationTargetException =>
        e.printStackTrace()
        throw new IOException()
    }
  }

  /** Write a BEGIN record for the specified transaction
    * @param tid The transaction that is beginning
    */
  def logXactionBegin(tid: TransactionId): Unit = this.synchronized {
    Debug.log("BEGIN")
    if (tidToFirstLogRecord.getOrElse(tid.id, null) != null) {
      println("logXactionBegin: already began this tid")
      throw new IOException("double logXactionBegin()")
    }
    preAppend()
    raf.writeInt(LogFile.BEGIN_RECORD)
    raf.writeLong(tid.id)
    raf.writeLong(currentOffset)
    tidToFirstLogRecord.put(tid.id, currentOffset)
    currentOffset = raf.getFilePointer

    Debug.log("BEGIN OFFSET = " + currentOffset)
  }

  /** Checkpoint the log and write a checkpoint record. */
  def logCheckpoint(): Unit = Database.getBufferPool().synchronized {
    // make sure we have buffer pool lock before proceeding
    this.synchronized {
      preAppend()
      val keys = tidToFirstLogRecord.keySet
      val els = keys.iterator
      force()
      Database.getBufferPool().flushAllPages()
      val startCpOffset = raf.getFilePointer
      raf.writeInt(LogFile.CHECKPOINT_RECORD)
      raf.writeLong(-1)  // no tid, but leave space for convenience

      // write list of outstanding transactions
      raf.writeInt(keys.size)
      while (els.hasNext) {
        val key = els.next
        Debug.log("WRITING CHECKPOINT TRANSACTION ID: " + key)
        raf.writeLong(key)
        raf.writeLong(tidToFirstLogRecord.getOrElse(key, null))
      }

      // once the CP is written, make sure the CP location at the beginning of the log file is updated
      val endCpOffset = raf.getFilePointer
      raf.seek(0)
      raf.writeLong(startCpOffset)
      raf.seek(endCpOffset)
      raf.writeLong(currentOffset)
      currentOffset = raf.getFilePointer
    }
    logTruncate()
  }

  /** Truncate any unneeded portion of the log to reduce its space
        consumption */
  def logTruncate(): Unit = this.synchronized {
    preAppend()
    raf.seek(0)
    val cpLoc = raf.readLong()
    var minLogRecord = cpLoc

    if (cpLoc != -1L) {
      raf.seek(cpLoc)
      val cpType = raf.readInt()
      val cpTid = raf.readLong()

      if (cpType != LogFile.CHECKPOINT_RECORD)
        throw new RuntimeException("Checkpoint pointer does not point to checkpoint record")

      val numOutstanding = raf.readInt()

      for (i <- 0 until numOutstanding) {
        val tid = raf.readLong()
        val firstLogRecord = raf.readLong()
        if (firstLogRecord < minLogRecord)
          minLogRecord = firstLogRecord
      }
    }

    // we can truncate everything before minLogRecord
    val newFile = new File("logtmp" + System.currentTimeMillis())
    val logNew = new RandomAccessFile(newFile, "rw")
    logNew.seek(0)
    logNew.writeLong(cpLoc - minLogRecord + LogFile.LONG_SIZE)

    raf.seek(minLogRecord)

    // have to rewrite log records since offsets are different after truncation
    var continue = true
    while (continue) {
      try {
        val typeInt = raf.readInt()
        val recordTid = raf.readLong()
        val newStart = logNew.getFilePointer

        Debug.log("NEW START = " + newStart)

        logNew.writeInt(typeInt)
        logNew.writeLong(recordTid)

        typeInt match {
          case LogFile.UPDATE_RECORD =>
            val before = readPageData(raf)
            val after = readPageData(raf)
            writePageData(logNew, before)
            writePageData(logNew, after)
          case LogFile.CHECKPOINT_RECORD =>
            val numXactions = raf.readInt()
            logNew.writeInt(numXactions)
            for (i <- numXactions until 0) {
              val xid = raf.readLong()
              val xoffset = raf.readLong()
              logNew.writeLong(xid)
              logNew.writeLong(xoffset - minLogRecord + LogFile.LONG_SIZE)
            }
          case LogFile.BEGIN_RECORD =>
            tidToFirstLogRecord.put(recordTid, newStart)
        }

        // all xactions finish with a pointer
        logNew.writeLong(newStart)
        raf.readLong()
      } catch {
        case EOFException => continue = false
      }
    }

    Debug.log(s"TRUNCATING LOG; WAS ${raf.length()} BYTES ;" +
              s"NEW START : $minLogRecord NEW LENGTH: ${raf.length() - minLogRecord}")

    raf.close()
    logFile.delete()
    newFile.renameTo(logFile)
    raf = new RandomAccessFile(logFile, "rw")
    raf.seek(raf.length())
    newFile.delete()
    currentOffset = raf.getFilePointer
  }

  /** Rollback the specified transaction, setting the state of any
    * of pages it updated to their pre-updated state.  To preserve
    * transaction semantics, this should not be called on
    * transactions that have already committed (though this may not
    * be enforced by this method.)
    *
    * @param tid The transaction to rollback
    */
  def rollback(tid: TransactionId): Unit = Database.getBufferPool().syncronized {
    this.synchronized {
      preAppend()
      // TODO
      ???
    }
  }

  /** Shutdown the logging system, writing out whatever state
    * is necessary so that start up can happen quickly (without
    * extensive recovery.)
    */
  def shutdown(): Unit = this.synchronized {
    try {
      logCheckpoint()
      raf.close()
    } catch {
      case e: IOException =>
        System.out.println("ERROR SHUTTING DOWN -- IGNORING.")
        e.printStackTrace()
    }
  }

  /** Recover the database system by ensuring that the updates of
    * committed transactions are installed and that the
    * updates of uncommitted transactions are not installed.
    */
  def recover(): Unit = Database.getBufferPool().syncronized {
    recoveryUndecided = false;
    // TODO
    ???
  }

  /** Print out a human readable representation of the log. */
  def print(): Unit = {
    // TODO
    ???
  }

  def force(): Unit = this.synchronized {
    raf.getChannel.force(true)
  }
}

object LogFile {
  final val ABORT_RECORD = 1
  final val COMMIT_RECORD = 2
  final val UPDATE_RECORD = 3
  final val BEGIN_RECORD = 4
  final val CHECKPOINT_RECORD = 5
  final val NO_CHECKPOINT_ID: Long = -1l

  val INT_SIZE = 4
  val LONG_SIZE = 8
}
