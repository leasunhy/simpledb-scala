package simpledb.systemtest

import org.scalatest.{BeforeAndAfter, FlatSpec}
import simpledb.Database

abstract class SimpleDbTestBase extends FlatSpec with BeforeAndAfter {
  /** Reset the database before each test is run. */
  before {
    Database.reset()
    beforeEachTest()
  }

  def beforeEachTest(): Unit = {}
}
