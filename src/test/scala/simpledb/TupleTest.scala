package simpledb

import simpledb.systemtest.SimpleDbTestBase
import org.scalatest.Assertions._

class TupleTest extends SimpleDbTestBase {
  "TupleDesc" should "have its getField() and setField() properly implemented." in {
    val td = Utility.getTupleDesc(2)

    val t = new Tuple(td)
    t.setField(0, new IntField(-1))
    t.setField(1, new IntField(0))
    assert(new IntField(-1) === t.getField(0))
    assert(new IntField(0) === t.getField(1))

    t.setField(0, new IntField(1))
    t.setField(1, new IntField(37))
    assert(new IntField(1) === t.getField(0))
    assert(new IntField(37) === t.getField(1))
  }

  it should "also have its getTupleDesc() properly implemented." in {
    val td = Utility.getTupleDesc(5)
    val t = new Tuple(td)
    assert(td === t.getTupleDesc())
  }

  it should "also have its getRecordId() and setRecordId() implemented." in {
    val t = new Tuple(Utility.getTupleDesc(1))
    val pid = new HeapPageId(0, 0)
    val rid = new RecordId(pid, 0)
    t.setRecordId(rid)
    try {
      assert(rid === t.getRecordId())
    } catch {
      case e: NotImplementedError =>
        throw new NotImplementedError("modifyRecordId() test failed due to " +
          "RecordId.equals() not being implemented.  This is not required for Lab 1, " +
          "but should pass when you do implement the RecordId class.")
    }
  }
}
