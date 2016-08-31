package simpledb.systemtest

import scala.util.Random

/**
  * Dumps the contents of a table.
  * args[1] is the number of columns.  E.g., if it's 5, then ScanTest will end
  * up dumping the contents of f4.0.txt.
  */
class ScanTest extends SimpleDbTestBase {
  val r = new Random()

  /** Tests the scan operator for a table with the specified dimensions. */
  def validateScan(columnSizes: Array[Int], rowSizes: Array[Int]) = {
    for (columns <- columnSizes; rows <- rowSizes) {
      val tuples = new
    }
  }
}
