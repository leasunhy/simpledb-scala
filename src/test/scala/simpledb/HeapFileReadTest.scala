package simpledb

import simpledb.systemtest.SimpleDbTestBase

class HeapFileReadTest extends SimpleDbTestBase {
  var hf: HeapFile = _
  var td: TupleDesc = _
  var tid: TransactionId = _

  before {
    val hf = SystemTestUtil.createRandomHeapFile(2, 20, null, null)
    val td = Utility.getTupleDesc(2)
    val tid = new TransactionId()
  }

  after {
    Database.getBufferPool.transactionComplete(tid)
  }

  "HeapFile" should "implement getId()" in {
    val id = hf.getId
    assert(id == hf.getId)
    assert(id == hf.getId)

    val other: HeapFile = SystemTestUtil.createRandomHeapFile(1, 1, null, null)
    assert(id !== other.getId)
  }

  it should "implement getTupleDesc()" in {
    assert(td === hf.getTupleDesc)
  }

  it should "implement numPages()" in {
    assert(1 === hf.numPages)
  }

  it should "implement readPage()" in {
    val smallFile = SystemTestUtil.createRandomHeapFile(1, 1, null, null)
    val it = smallFile.iterator(tid)
    // not opened yet
    assert(!it.hasNext())
    assertThrows[NoSuchElementException](it.next())

    it.open()
    var count = 0
    while (it.hasNext()) {
      assert(it.next() !== null)
      count += 1
    }
    assert(3 === count)
    it.close()
  }

  it should "implement close() of iterator" in {
    // make more than 1 page. Previous closed iterator would start fetching from page 1.
    val twoPageFile = SystemTestUtil.createRandomHeapFile(2, 520, null, null)
    val it = twoPageFile.iterator(tid)
    it.open()
    assert(it.hasNext())
    it.close()
    assertThrows[NoSuchElementException](it.next())
    // closing it twice is harmless
    it.close()
  }
}
