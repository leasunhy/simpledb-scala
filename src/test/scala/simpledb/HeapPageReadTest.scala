package simpledb

import simpledb.systemtest.SimpleDbTestBase
import java.io.{File, IOException}
import java.util.UUID

import simpledb.TestUtil.SkeletonFile

import scala.collection.mutable.ArrayBuffer

class HeapPageReadTest extends SimpleDbTestBase {
  val pid = new HeapPageId(-1, -1)
  val EXAMPLE_VALUES = HeapPageReadTest.EXAMPLE_VALUES
  val EXAMPLE_DATA = HeapPageReadTest.EXAMPLE_DATA

  override def beforeEachTest(): Unit = {
    super.beforeEachTest()
    Database.getCatalog.addTable(new SkeletonFile(-1, Utility.getTupleDesc(2)), UUID.randomUUID().toString)
  }

  "HeapPage" should "implement getId()" in {
    val page = new HeapPage(pid, EXAMPLE_DATA)
    assert(pid === page.getId)
  }

  it should "implement iterator()" in {
    val page = new HeapPage(pid, EXAMPLE_DATA)
    val it = page.iterator
    for ((tup, i) <- it.zipWithIndex) {
      val f0 = tup.getField(0).asInstanceOf[IntField]
      val f1 = tup.getField(1).asInstanceOf[IntField]
      assert(EXAMPLE_VALUES(i)(0) === f0.value)
      assert(EXAMPLE_VALUES(i)(1) === f1.value)
    }
  }

  it should "implement getNumEmptySlots()" in {
    val page = new HeapPage(pid, EXAMPLE_DATA)
    assert(484 === page.getNumEmptySlots)
  }

  it should "implement getSlot()" in {
    val page = new HeapPage(pid, EXAMPLE_DATA)
    (0 until 20).foreach(i => assert(page.getSlot(i) === true))
    (20 until 504).foreach(i => assert(page.getSlot(i) === false))
  }
}

object HeapPageReadTest {
  val EXAMPLE_VALUES = ArrayBuffer(
    ArrayBuffer(31933, 862),
    ArrayBuffer(29402, 56883),
    ArrayBuffer(1468, 5825),
    ArrayBuffer(17876, 52278),
    ArrayBuffer(6350, 36090),
    ArrayBuffer(34784, 43771),
    ArrayBuffer(28617, 56874),
    ArrayBuffer(19209, 23253),
    ArrayBuffer(56462, 24979),
    ArrayBuffer(51440, 56685),
    ArrayBuffer(3596, 62307),
    ArrayBuffer(45569, 2719),
    ArrayBuffer(22064, 43575),
    ArrayBuffer(42812, 44947),
    ArrayBuffer(22189, 19724),
    ArrayBuffer(33549, 36554),
    ArrayBuffer(9086, 53184),
    ArrayBuffer(42878, 33394),
    ArrayBuffer(62778, 21122),
    ArrayBuffer(17197, 16388)
  )

  val EXAMPLE_DATA: Array[Byte] = try {
    val temp = File.createTempFile("table", ".dat")
    temp.deleteOnExit()
    HeapFileEncoder.convert(EXAMPLE_VALUES, temp, BufferPool.PAGE_SIZE, 2)
    TestUtil.readFileBytes(temp.getAbsolutePath)
  } catch {
    case e: IOException => throw new RuntimeException()
  }
}
