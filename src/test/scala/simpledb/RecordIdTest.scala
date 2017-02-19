package simpledb

import simpledb.systemtest.SimpleDbTestBase
import org.scalatest.Assertions._

class RecordIdTest extends SimpleDbTestBase {
  var hrid : RecordId = _
  var hrid2: RecordId = _
  var hrid3: RecordId = _
  var hrid4: RecordId = _

  before {
    val hpid = new HeapPageId(-1, 2)
    val hpid2 = new HeapPageId(-1, 2)
    val hpid3 = new HeapPageId(-2, 2)
    hrid = new RecordId(hpid, 3)
    hrid2 = new RecordId(hpid2, 3)
    hrid3 = new RecordId(hpid, 4)
    hrid4 = new RecordId(hpid3, 3)
  }

  "RecordId" should "have its getPageId() implemented." in {
    assert(new HeapPageId(-1, 2) === hrid.getPageId)
  }

  it should "implement tupleno()." in {
    assert(3 === hrid.getTupleNo)
  }

  it should "implement equals()." in {
    assert(hrid === hrid2)
    assert(hrid2 === hrid)
    assert(!hrid.equals(hrid3))
    assert(!hrid3.equals(hrid))
    assert(!hrid2.equals(hrid4))
    assert(!hrid4.equals(hrid2))
  }

  it should "implement hashCode()." in {
    assert(hrid.hashCode() === hrid2.hashCode())
  }
}
