package simpledb.systemtest

import java.io.File
import java.util.UUID

import simpledb._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

object SystemTestUtil {
  val SINGLE_INT_DESCRIPTOR = new TupleDesc(Array(Type.IntType))
  val MAX_RAND_VALUE = 1 << 16

  /** @param columnSpecification Mapping between column index and value. */
  def createRandomHeapFile(columns: Int,
                           rows: Int,
                           columnSpecification: Map[Int, Int],
                           tuples: mutable.Buffer[IndexedSeq[Int]],
                           maxValue: Int = MAX_RAND_VALUE,
                           colPrefix: String = null) = {
    val temp = createRandomHeapFileUnopened(columns, rows, columnSpecification, tuples, maxValue)
    if (colPrefix == null)
      Utility.openHeapFile(columns, temp)
    else
      Utility.openHeapFile(columns, colPrefix, temp)
  }

  def createRandomHeapFileUnopened(columns: Int, rows: Int, columnSpecification: Map[Int, Int],
                                   tuples: ArrayBuffer[IndexedSeq[Int]], maxValue: Int): File = {
    val r = new Random()
    if (tuples != null)
      tuples.clear()
    val newTuples = if (tuples == null) ArrayBuffer.empty[IndexedSeq[Int]] else tuples
    tuples = columnSpecification match {
      case null => ArrayBuffer.fill[Int](rows, columns) { r.nextInt(maxValue) }
      case _ => ArrayBuffer.tabulate[Int](rows, columns) { (i, _) => columnSpecification.getOrElse(i, 0) }
    }
    // convert the tuple list to a heap file and open i
    val temp = File.createTempFile("table", ".dat")
    temp.deleteOnExit()
    HeapFileEncoder.convert(newTuples, temp, BufferPool.PAGE_SIZE, columns)
    temp
  }

  def tupleToList(tuple: Tuple): IndexedSeq[Int] =
    (0 until tuple.getTupleDesc().numFields).map(tuple.getField(_).asInstanceOf[IntField].value)

  def listToTuple(values: IndexedSeq[Int]): Tuple = {
    val t = new Tuple(Utility.getTupleDesc(values.length))
    values.view.zipWithIndex.foreach{ case (v, i) => t.setField(i, new IntField(v)) }
    t
  }

  def matchTuples(f: DbFile, tuples: IndexedSeq[IndexedSeq[Int]]): Unit = {
    val tid = new TransactionId()
    matchTuples(f, tid, tuples)
    Database.getBufferPool.transactionComplete(tid)
  }

  def matchTuples(f: DbFile, tid: TransactionId, tuples: IndexedSeq[IndexedSeq[Int]]): Unit = {
    val scan = new SeqScan(tid, f.getId, "")
    matchTuples(scan, tuples)
  }

  def matchTuples(iterator: DbIterator, tuples: IndexedSeq[IndexedSeq[Int]]): Unit = {
    if (Debug.isEnabled()) {
      Debug.log("Expected tuples:")
      for (t <- tuples)
        Debug.log(s"\t ${Utility.listToString(t)}")
    }

    val copy = ArrayBuffer(tuples: _*)
    iterator.open()
    for (t <- iterator) {
      val list = tupleToList(t)
      val index = copy.indexOf(list)
      val isExpected = index != -1
      if (isExpected)
        copy.remove(index)
      Debug.log("scanned tuple: %s (%s)", t, if (isExpected) "expected" else "not expected")
      assert(isExpected, "expected tuples does not contain: " + t)
    }
    iterator.close()

    val MAX_TUPLES_OUTPUT = 10
    val msg = "expected to find the following tuples:\n"
    val moreMsg = s"[${copy.size - MAX_TUPLES_OUTPUT} more tuples]"
    val tupleMsg = copy.view.map(t => s"\t${Utility.listToString(t)}\n").mkString("")
    assert(copy.isEmpty, msg + tupleMsg + moreMsg)
  }

  /**
    * Returns number of bytes of RAM used by JVM after calling System.gc many times.
    * @return amount of RAM (in bytes) used by JVM
    */
  def getMemoryFootprint: Long = {
    val runtime = Runtime.getRuntime
    var memAfter = runtime.totalMemory() - runtime.freeMemory()
    var memBefore = memAfter + 1
    while (memBefore != memAfter) {
      memBefore = memAfter
      System.gc()
      memAfter = runtime.totalMemory() - runtime.freeMemory()
    }
    memAfter
  }

  /**
    * Generates a unique string each time it is called.
    * @return a new unique UUID as a string, using java.util.UUID
    */
  def getUUID(): String = UUID.randomUUID().toString

  def getDiff(sequence: Array[Double]): Array[Double] = (sequence.view.drop(1), sequence).zipped.map(_ - _).toArray

  /**
    * Checks if the sequence represents a quadratic sequence (approximately)
    * ret[0] is true if the sequence is quadratic
    * ret[1] is the common difference of the sequence if ret[0] is true.
    * @param sequence
    * @return ret[0] = true if sequence is quadratic(or sub-quadratic or linear), ret[1] = the coefficient of n^2
    */
  def checkQuadratic(sequence: Array[Double]): (Boolean, Double) = {
    val ret = checkLinear(getDiff(sequence))
    (ret._1, ret._2 / 2.0)
  }

  /**
    * Checks if the sequence represents an arithmetic sequence (approximately)
    * ret[0] is true if the sequence is linear
    * ret[1] is the common difference of the sequence if ret[0] is true.
    * @param sequence
    * @return ret[0] = true if sequence is linear, ret[1] = the common difference
    */
  def checkLinear(sequence: Array[Double]): (Boolean, Double) = checkConstant(getDiff(sequence))

  /**
    * Checks if the sequence represents approximately a fixed sequence (c,c,c,c,..)
    * ret[0] is true if the sequence is linear
    * ret[1] is the constant of the sequence if ret[0] is true.
    * @param sequence
    * @return ret[0] = true if sequence is constant, ret[1] = the constant
    */
  def checkConstant(sequence: Array[Double]): (Boolean, Double) = {
    val av = sequence.sum
    val sqsum = sequence.view.map(_ - av).map(v => v * v).sum
    val std = math.sqrt(sqsum / (sequence.length + .0))
    (std < 1.0, av)
  }
}
