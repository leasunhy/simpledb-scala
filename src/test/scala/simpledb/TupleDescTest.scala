package simpledb

import simpledb.systemtest.SimpleDbTestBase

class TupleDescTest extends SimpleDbTestBase {
  private def combinedStringArrays(td1: TupleDesc, td2: TupleDesc, combined: TupleDesc): Boolean = {
    if (!(0 until td1.numFields).forall(i => td1.getType(i) == combined.getType(i)))
      false
    else
      (td1.numFields until (td1.numFields + td2.numFields)).forall {
        i => td2.getType(i - td1.numFields) == combined.getType(i)
      }
  }

  "TupleDesc" should "implement combine()" in {
    def testCombine(td1: TupleDesc, td2: TupleDesc, expectedNumFields: Int) = {
      val td3 = TupleDesc.combine(td1, td2)
      assert(td3.numFields == expectedNumFields)
      assert(expectedNumFields * Type.IntType.getLen === td3.getSize)
      (0 until expectedNumFields).foreach(i => assert(td3.getType(i) === Type.IntType))
      assert(combinedStringArrays(td1, td2, td3))
    }
    val td1 = Utility.getTupleDesc(1, "td1")
    val td2 = Utility.getTupleDesc(2, "td2")
    testCombine(td1, td2, 3)
    testCombine(td2, td1, 3)
    testCombine(td2, td2, 4)
  }

  it should "implement getType()" in {
    val lengths = Array(1, 2, 1000)
    for (len <- lengths) {
      val td = Utility.getTupleDesc(len)
      (0 until len) foreach { i => assert(td.getType(i) === Type.IntType) }
    }
  }

  it should "implement nameToId()" in {
    val lengths = Array(1, 2, 1000)
    val prefix = "test"
    for (len <- lengths) {
      // Make sure you retrieve well-named fields
      val td = Utility.getTupleDesc(len, prefix)
      (0 until len).foreach(i => assert(td.nameToId(prefix + i) === i))

      // Make sure you throw exception for non-existent fields
      assertThrows[NoSuchElementException](td.nameToId("foo"))

      // Make sure you throw exception for null searches
      assertThrows[NoSuchElementException](td.nameToId(null))

      // Make sure you throw exception when all field names are null
      val td1 = Utility.getTupleDesc(len)
      assertThrows[NoSuchElementException](td.nameToId(prefix))
    }
  }

  it should "implement getSize()" in {
    val lengths = Array(1, 2, 1000)
    for (len <- lengths) {
      val td = Utility.getTupleDesc(len)
      assert(len * Type.IntType.getLen === td.getSize)
    }
  }

  it should "implement numFields()" in {
    val lengths = Array(1, 2, 1000)
    for (len <- lengths) {
      val td = Utility.getTupleDesc(len)
      assert(len === td.numFields)
    }
  }

  it should "implement equals()" in {
    val singleInt = new TupleDesc(Array(Type.IntType))
    val singleInt2 = new TupleDesc(Array(Type.IntType))
    val intString = new TupleDesc(Array(Type.IntType, Type.StringType))

    assert(!singleInt.equals(null))
    assert(!singleInt.equals(new Object))

    assert(singleInt.equals(singleInt))
    assert(singleInt.equals(singleInt2))
    assert(singleInt2.equals(singleInt))
    assert(intString.equals(intString))

    assert(!singleInt.equals(intString))
    assert(!singleInt2.equals(intString))
    assert(!intString.equals(singleInt))
    assert(!intString.equals(singleInt2))
  }
}
