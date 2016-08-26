package simpledb

import java.io.{File, FileInputStream, IOException}

import scala.collection.mutable.ArrayBuffer
import org.scalatest.Assertions._

object TestUtil {
  /**
    * @return an IntField with value n
    */
  def getField(n: Int): Field = new IntField(n)

  /**
    * @return a DbIterator over a list of tuples constructed over the data
    *   provided in the constructor. This iterator is already open.
    * @param width the number of fields in each tuple
    * @param tupleData an array such that the ith element the jth tuple lives
    *   in slot j * width + i
    * @throws DbException if we encounter an error creating the
    *   TupleIterator
    */
  def createTupleList(width: Int, tupleData: Array[Int]): TupleIterator = {
    val tupleList = ArrayBuffer.empty[Tuple]
    var i = 0
    while (i < tupleData.length) {
      val t = new Tuple(Utility.getTupleDesc(width))
      for (j <- 0 until width) {
        t.setField(j, getField(i))
        i += 1
      }
      tupleList += t
    }
    val result = new TupleIterator(Utility.getTupleDesc(width), tupleList)
    result.open()
    result
  }

  /**
    * @return a DbIterator over a list of tuples constructed over the data
    *   provided in the constructor. This iterator is already open.
    * @param width the number of fields in each tuple
    * @param tupleData an array such that the ith element the jth tuple lives
    *   in slot j * width + i.  Objects can be strings or ints;  tuples must all be of same type.
    * @require tupdata.length % width == 0
    * @throws DbException if we encounter an error creating the
    *   TupleIterator
    */
  def createTupleList(width: Int, tupleData: Array[Object]): TupleIterator = {
    val tupleList = ArrayBuffer.empty[Tuple]
    val types = tupleData.map {
      case _: String  => Type.StringType
      case _: Integer => Type.IntType
      case _          => null
    }
    val td = new TupleDesc(types)

    var i = 0
    while (i < tupleData.length) {
      val t = new Tuple(td)
      for (j <- 0 until width) {
        val f = tupleData(i) match {
          case s: String  => new StringField(s, Type.StringType.STRING_LEN)
          case i: Integer => new IntField(i)
        }
        t.setField(j, f)
        i += 1
      }
    }
    val result = new TupleIterator(td, tupleList)
    result.open()
    result
  }

  /**
    * @return true iff the tuples have the same number of fields and
    *   corresponding fields in the two Tuples are all equal.
    */
  def compareTuples(t1: Tuple, t2: Tuple): Boolean = {
    val td1 = t1.getTupleDesc()
    val td2 = t2.getTupleDesc()
    (td1.numFields == td2.numFields
      && (0 until td1.numFields).forall((i: Int) => td1.getType(i) eq td2.getType(i))
      && (0 until td1.numFields).forall((i: Int) => t1.getField(i) == t2.getField(i))
      )
  }

  /**
    * Check to see if the DbIterators have the same number of tuples and
    *   each tuple pair in parallel iteration satisfies compareTuples .
    * If not, throw an assertion.
    */
  def compareDbIterators(expected: DbIterator, actual: DbIterator): Unit = {
    while (expected.hasNext()) {
      assert(actual.hasNext())
      val expectedTuple = expected.next()
      val actualTuple = actual.next()
      assert(compareTuples(expectedTuple, actualTuple))
    }
    // Both must now be exhausted
    assert(!expected.hasNext())
    assert(!actual.hasNext())
  }

  /**
    * Check to see if every tuple in expected matches <b>some</b> tuple
    *   in actual via compareTuples. Note that actual may be a superset.
    * If not, throw an assertion.
    */
  def matchAllTuples(expected: DbIterator, actual: DbIterator): Unit = {
    // NOTE(leasunhy): the following two lines are comments from original java version of simpledb
    // TODO(ghuo): this n^2 set comparison is kind of dumb, but we haven't
    // implemented hashCode or equals for tuples.
    var matched = false
    while (expected.hasNext()) {
      val expectedTuple = expected.next()
      matched = false
      actual.rewind()

      while (!matched && actual.hasNext())
        matched = compareTuples(expectedTuple, actual.next())

      if (!matched)
        throw new RuntimeException("Expected tuple not found: " + expectedTuple)
    }

    /**
      * Verifies that the DbIterator has been exhausted of all elements.
      */
    def readFileBytes(path: String): Array[Byte] = {
      val f = new File(path)
      val is = new FileInputStream(f)
      val buf = Array.ofDim[Byte](f.length().toInt)

      var offset = 0
      var count = 0
      while (offset < buf.length) {
        count = is.read(buf, offset, buf.length - offset)
        if (count >= 0)
          offset += count
      }

      // check that we grabbed the entire file
      if (offset < buf.length)
        throw new IOException("failed to read test data")

      // Close the input stream and return bytes
      is.close()
      buf
    }

    /**
      * Stub DbFile class for unit testing.
      */
    class SkeletonFile(tableId: Int, td: TupleDesc) extends DbFile {
      override def readPage(id: PageId): Page = ???

      override def writePage(p: Page): Unit = ???

      override def addTuple(tid: TransactionId, t: Tuple): Vector[Page] = ???

      override def iterator(tid: TransactionId): DbFileIterator = ???

      override def getId: Int = tableId

      override def getTupleDesc: TupleDesc = td
    }

    /**
      * Mock SeqScan class for unit testing.
      */
    class MockScan(low: Int, high: Int, width: Int) extends DbIterator {
      var cur = low

      override def open(): Unit = {}

      override def hasNext(): Boolean = cur < high

      override def next(): Tuple = {
        if (cur >= high)
          throw new NoSuchElementException()
        val t = new Tuple(getTupleDesc)
        for (i <- 0 until width)
          t.setField(i, new IntField(cur))
        cur += 1
        t
      }

      override def rewind(): Unit = cur = low

      override def getTupleDesc: TupleDesc = Utility.getTupleDesc(width)

      override def close(): Unit = {}
    }

    /**
      * Helper class that attempts to acquire a lock on a given page in a new
      * thread.
      *
      * @return a handle to the Thread that will attempt lock acquisition after it
      *   has been started
      * @constructor Constructor.
      * @param tid the transaction on whose behalf we want to acquire the lock
      * @param pid the page over which we want to acquire the lock
      * @param perm the desired lock permissions
      */
    class LockGrabber(tid: TransactionId, pid: PageId, perm: Permissions) extends Thread {
      private var acquired = false
      private var error: Exception = _
      private val alock = new Object
      private val elock = new Object

      override def run(): Unit = {
        try {
          Database.getBufferPool.getPage(tid, pid, perm)
          alock.synchronized { acquired = true }
        } catch {
          case e: Exception =>
            e.printStackTrace()
            elock.synchronized { error = e }
            try {
              Database.getBufferPool.transactionComplete(tid, false)
            } catch {
              case e2: IOException => e.printStackTrace()
            }
        }
      }

      /**
        * @return true if we successfully acquired the specified lock
        */
      def getAcquired = alock.synchronized { acquired }

      /**
        * @return an Exception instance if one occurred during lock acquisition;
        *   null otherwise
        */
      def getError = elock.synchronized { error }
    }


    /** JUnit fixture that creates a heap file and cleans it up afterward. */
    abstract class CreateHeapFile protected() {
      protected var empty: HeapFile = _

      private final val emptyFile = try {
        File.createTempFile("empty", ".dat")
      } catch {
        case e: IOException =>
          throw new RuntimeException(e)
          null
      }
      emptyFile.deleteOnExit()

      protected def setUp(): Unit = {
        try {
          Database.reset()
          empty = Utility.createEmptyHeapFile(emptyFile.getAbsolutePath, 2)
        } catch {
          case e: IOException =>
            throw new RuntimeException(e)
        }
      }
    }
  }
}
