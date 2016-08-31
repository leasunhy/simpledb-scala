package simpledb

import java.io.{File, FileOutputStream, IOException}
import java.util.UUID

/** Helper methods used for testing and implementing random features. */
object Utility {
  /**
    * @return a Type array of length len populated with Type.INT_TYPE
    */
  def getTypes(len: Int): Array[Type] = Array.fill[Type](len)(Type.IntType)

  /**
    * @return a String array of length len populated with the (possibly null) strings in val,
    * and an appended increasing integer at the end (val1, val2, etc.).
    */
  def getStrings(len: Int, value: String): Array[String] = Array.tabulate(len)(i => value + i)

  /**
    * @return a TupleDesc with n fields of type Type.INT_TYPE, each named
    * name + n (name1, name2, etc.).
    */
  def getTupleDesc(n: Int, name: String): TupleDesc = new TupleDesc(getTypes(n), getStrings(n, name))

  /**
    * @return a TupleDesc with n fields of type Type.INT_TYPE
    */
  def getTupleDesc(n: Int): TupleDesc = new TupleDesc(getTypes(n))

  /**
    * @return a Tuple with a single IntField with value n and with
    *   RecordId(HeapPageId(1,2), 3)
    */
  def getHeapTuple(n: Int): Tuple = {
    val t = new Tuple(getTupleDesc(1))
    t.setRecordId(new RecordId(new HeapPageId(1, 2), 3))
    t.setField(0, new IntField(n))
    t
  }

  /**
    * @return a Tuple with an IntField for every element of tupdata
    *   and RecordId(HeapPageId(1, 2), 3)
    */
  def getHeapTuple(tupleData: Array[Int]): Tuple = {
    val t = new Tuple(getTupleDesc(tupleData.length))
    t.setRecordId(new RecordId(new HeapPageId(1, 2), 3))
    tupleData.indices.foreach(i => t.setField(i, new IntField(tupleData(i))))
    t
  }

  /**
    * @return a Tuple with a 'width' IntFields each with value n and
    *   with RecordId(HeapPageId(1, 2), 3)
    */
  def getHeapTuple(n: Int, width: Int): Tuple = {
    val t = new Tuple(getTupleDesc(width))
    t.setRecordId(new RecordId(new HeapPageId(1, 2), 3))
    (0 until width).indices.foreach(i => t.setField(i, new IntField(n)))
    t
  }

  /**
    * @return a Tuple with a 'width' IntFields with the value tupledata[i]
    *         in each field.
    *         do not set it's RecordId, hence do not distinguish which
    *         sort of file it belongs to.
    */
  def getTuple(tupleData: Array[Int], width: Int): Tuple = {
    if (tupleData.length != width) {
      System.out.println("get Hash Tuple has the wrong length~")
      System.exit(1)
    }
    val t = new Tuple(getTupleDesc(width))
    (0 until width).foreach(i => t.setField(i, new IntField(tupleData(i))))
    t
  }

  /**
    * A utility method to create a new HeapFile with a single empty page,
    * assuming the path does not already exist. If the path exists, the file
    * will be overwritten. The new table will be added to the Catalog with
    * the specified number of columns as IntFields.
    */
  def createEmptyHeapFile(path: String, cols: Int): HeapFile = {
    val f = new File(path)
    // touch the file
    val fos = new FileOutputStream(f)
    fos.write(Array.emptyByteArray)
    fos.close()

    val hf = openHeapFile(cols, f)
    val pid = new HeapPageId(hf.getId, 0)

    val page = try {
       new HeapPage(pid, HeapPage.createEmptyPageData())
    } catch {
      case _: IOException =>
        throw new RuntimeException("failed to create empty page in HeapFile")
        null
    }
    hf.writePage(page)
    hf
  }

  /** Opens a HeapFile and adds it to the catalog.
    *
    * @param cols number of columns in the table.
    * @param f location of the file storing the table.
    * @return the opened table.
    */
  def openHeapFile(cols: Int, f: File): HeapFile = {
    // create the HeapFile and add it to the catalog
    val td = getTupleDesc(cols)
    val hf = new HeapFile(f, td)
    Database.getCatalog.addTable(hf, UUID.randomUUID().toString)
    hf
  }

  def openHeapFile(cols: Int, colPrefix: String, f: File): HeapFile = {
    val td = getTupleDesc(cols, colPrefix)
    val hf = new HeapFile(f, td)
    Database.getCatalog.addTable(hf, UUID.randomUUID().toString)
    hf
  }

  def listToString(list: IndexedSeq[Int]): String = list.mkString("\t")
}

