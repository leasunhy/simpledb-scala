package simpledb

import java.io.{File, IOException}

/** Database is a class that initializes several static
  * variables used by the database system (the catalog, the buffer pool,
  * and the log files, in particular.)
  * <p>
  * Provides a set of methods that can be used to access these variables
  * from anywhere.
  */
class Database private() {
  private final val catalog = new Catalog()
  private var bufferPool = new BufferPool(BufferPool.DEFAULT_PAGES)

  private final val LOG_FILENAME = "log"

  private val logFile = try {
    new LogFile(new File(LOG_FILENAME))
  } catch {
    case e: IOException =>
      e.printStackTrace()
      System.exit(1)
      null
  }
  // startControllerThread()
}

object Database {
  private var instance = new Database()

  /** Return the log file of the static Database instance*/
  def getLogFile: LogFile = instance.logFile

  /** Return the buffer pool of the static Database instance*/
  def getBufferPool: BufferPool = instance.bufferPool

  /** Return the catalog of the static Database instance*/
  def getCatalog: Catalog = instance.catalog

  /** Method used for testing -- create a new instance of the buffer pool and return it. */
  def resetBufferPool(pages: Int): BufferPool = {
    instance.bufferPool = new BufferPool(pages)
    instance.bufferPool
  }

  //reset the database, used for unit tests only.
  def reset(): Unit = instance = new Database()
}
