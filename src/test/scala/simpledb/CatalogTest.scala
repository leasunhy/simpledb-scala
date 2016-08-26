package simpledb

import java.util.UUID

import simpledb.systemtest.SimpleDbTestBase

class CatalogTest extends SimpleDbTestBase {
  val name = "test"
  var nameThisRun = ""

  before {
    Database.getCatalog.clear()
    nameThisRun = UUID.randomUUID().toString
    Database.getCatalog.addTable(new TestUtil.SkeletonFile(-1, Utility.getTupleDesc(2)), nameThisRun)
    Database.getCatalog.addTable(new TestUtil.SkeletonFile(-2, Utility.getTupleDesc(2)), name)
  }

  "Catalog" should "implement getTupleDesc()" in {
    val expected = Utility.getTupleDesc(2)
    val actual = Database.getCatalog.getTupleDesc(-1)
    assert(expected === actual)
  }

  it should "implement getTableName()" in {
    assert(nameThisRun === Database.getCatalog.getTableName(-1))
    assert(name === Database.getCatalog.getTableName(-2))
  }

  it should "implement getTableId()" in {
    assert(-1 === Database.getCatalog.getTableId(nameThisRun))
    assert(-2 === Database.getCatalog.getTableId(name))

    assertThrows[NoSuchElementException](Database.getCatalog.getTableId(null))
    assertThrows[NoSuchElementException](Database.getCatalog.getTableId("foo"))
  }

  it should "implement getDbFile()" in {
    val f = Database.getCatalog.getDbFile(-1)
    assert(-1 === f.getId)
  }
}
