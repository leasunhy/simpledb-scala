package simpledb

import simpledb.systemtest.SimpleDbTestBase

class HeapPageIdTest extends SimpleDbTestBase {
  val pid = new HeapPageId(1, 1)

  "HeapPageId" should "implement getTableId()" in {
    assert(1 === pid.getTableId)
  }

  it should "implement pageno()" in {
    assert(1 === pid.getPageNo)
  }

  it should "implement hashCode()" in {
    // testing determinism
    val code = pid.hashCode()
    assert(pid.hashCode() === code)
    assert(pid.hashCode() === code)
  }

  it should "implement equals()" in {
    val pid1 = new HeapPageId(1, 1)
    val pid1Copy = new HeapPageId(1, 1)
    val pid2 = new HeapPageId(2, 2)

    assert(!pid1.equals(null))
    assert(!pid1.equals(new Object))

    assert(pid1.equals(pid1))
    assert(pid1.equals(pid1Copy))
    assert(pid1Copy.equals(pid1))
    assert(pid2.equals(pid2))

    assert(!pid1.equals(pid2))
    assert(!pid1Copy.equals(pid2))
    assert(!pid2.equals(pid1))
    assert(!pid2.equals(pid1Copy))
  }
}
