package simpledb.systemtest

import java.io.File

import simpledb._

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
  * Dumps the contents of a table.
  * args[1] is the number of columns.  E.g., if it's 5, then ScanTest will end
  * up dumping the contents of f4.0.txt.
  */
class ScanTest extends SimpleDbTestBase {
  val r = new Random()

  /** Tests the scan operator for a table with the specified dimensions. */
  private def validateScan(columnSizes: Array[Int], rowSizes: Array[Int]) = {
    for (columns <- columnSizes; rows <- rowSizes) {
      val tuples = ArrayBuffer.empty[ArrayBuffer[Int]]
      val f = SystemTestUtil.createRandomHeapFile(columns, rows, null, tuples)
      SystemTestUtil.matchTuples(f, tuples)
      Database.resetBufferPool(BufferPool.DEFAULT_PAGES)
    }
  }

  /* Scan 1-4 columns. */
  "The system" should "be able to scan small tables correctly." in {
    val columnSizes = Vector(1, 2, 3, 4)
    val rowSizes = Vector(0, 1, 2, 511, 512, 513, 1023, 1024, 1025, 4096 + r.nextInt(4096))
  }

  "A SeqScan iterator" should "work as expected." in {
    val tuples = ArrayBuffer.empty[ArrayBuffer[Int]]
    val f = SystemTestUtil.createRandomHeapFile(2, 1000, null, tuples)
    val tid = new TransactionId()
    val scan = new SeqScan(tid, f.getId, "table")
    scan.open()
    for (i <- 0 until 100) {
      assert(scan.hasNext())
      val t = scan.next()
      assert(tuples(i) === SystemTestUtil.tupleToList(t))
    }
    scan.rewind()
    for (i <- 0 until 100) {
      assert(scan.hasNext())
      val t = scan.next()
      assert(tuples(i) === SystemTestUtil.tupleToList(t))
    }
    scan.close()
    Database.getBufferPool.transactionComplete(tid)
  }

  "The buffer pool" should "actually cache data." in {
    /* Counts the number of readPage operations. */
    class InstrumentedHeapFile(f: File, td: TupleDesc) extends HeapFile(f, td) {
      var readCount = 0
      override def readPage(id: PageId): Page = {
        readCount += 1
        super.readPage(id)
      }
    }

    // Create the table
    val PAGES = 30
    val tuples = ArrayBuffer.empty[ArrayBuffer[Int]]
    val f = SystemTestUtil.createRandomHeapFileUnopened(1, 992 * PAGES, null, tuples, 1000)
    val td = Utility.getTupleDesc(1)
    val table = new InstrumentedHeapFile(f, td)
    Database.getCatalog.addTable(table, SystemTestUtil.getUUID())

    // Scan the table once
    SystemTestUtil.matchTuples(table, tuples)
    assert(PAGES === table.readCount)
    table.readCount = 0

    // Scan the table again: all pages should be cached
    SystemTestUtil.matchTuples(table, tuples)
    assert(0 === table.readCount)
  }
}
