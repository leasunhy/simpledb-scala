package simpledb

import java.io._
import java.util.NoSuchElementException

/**
  * HeapPage stores pages of HeapFiles and implements the Page interface that
  * is used by BufferPool.
  *
  * @see HeapFile
  * @see BufferPool
  * @constructor Create a HeapPage from a set of bytes of data read from disk.
  *              The format of a HeapPage is a set of header bytes indicating
  *              the slots of the page that are in use, some number of tuple slots.
  *               Specifically, the number of tuples is equal to: <p>
  *                       floor((BufferPool.PAGE_SIZE*8) / (tuple size * 8 + 1))
  *              <p> where tuple size is the size of tuples in this
  *              database table, which can be determined via [[Catalog.getTupleDesc]].
  *              The number of 8-bit header words is equal to:
  *              <p>
  *                   ceiling(no. tuple slots / 8)
  *              <p>
  * @see Database#getCatalog
  * @see Catalog#getTupleDesc
  * @see BufferPool#PAGE_SIZE
  */
class HeapPage(id: HeapPageId, data: Array[Byte]) extends Page {
  val pid = id
  val td = Database.getCatalog.getTupleDesc(id.getTableId)
  val numSlots = getNumTuples
  val dis = new DataInputStream(new ByteArrayInputStream(data))

  // allocate and read the header slots of this page
  val header = Array.fill[Byte](getHeaderSize)(dis.readByte)

  val tuples = try {
    // allocate and read the actual records of this page
    (0 until numSlots).map(readNextTuple(dis, _)).toArray
  } catch {
    case e: NoSuchElementException =>
      e.printStackTrace()
      null
  }

  dis.close()
  setBeforeImage()

  var oldData: Array[Byte] = _

  /** Retrieve the number of tuples on this page.
        @return the number of tuples on this page
    */
  private def getNumTuples: Int = {
    // TODO
    ???
    0
  }

  /**
    * Computes the number of bytes in the header of a page in a HeapFile with each tuple occupying tupleSize bytes
    * @return the number of bytes in the header of a page in a HeapFile with each tuple occupying tupleSize bytes
    */
  private def getHeaderSize: Int = {
    // TODO
    ???
    0
  }

  /** Return a view of this page before it was modified -- used by recovery */
  def getBeforeImage: HeapPage = try {
    new HeapPage(pid, oldData)
  } catch  {
    case e: IOException =>
      e.printStackTrace()
      //should never happen -- we parsed it OK before!
      System.exit(1)
      null
  }

  def setBeforeImage(): Unit = oldData = getPageData.clone()

  /**
    * @return the PageId associated with this page.
    */
  def getId: HeapPageId = {
    // TODO
    ???
  }

  /**
    * Suck up tuples from the source file.
    */
  private def readNextTuple(dis: DataInputStream, slotId: Int) = getSlot(slotId) match {
    // if associated bit is not set, read forward to the next tuple, and return null.
    case false =>
      (0 until td.getSize).foreach(_ => dis.readByte)
      null
    case true =>
      val t = new Tuple(td)
      val rid = new RecordId(pid, slotId)
      t.setRecordId(rid)
      try {
        for (j <- 0 until td.numFields)
          t.setField(j, td.getType(j).parse(dis))
      } catch {
        case e: java.text.ParseException =>
          e.printStackTrace()
          throw new NoSuchElementException("parsing error!")
      }
      t
  }

  /**
    * Generates a byte array representing the contents of this page.
    * Used to serialize this page to disk.
    * <p>
    * The invariant here is that it should be possible to pass the byte
    * array generated by getPageData to the HeapPage constructor and
    * have it produce an identical HeapPage object.
    *
    * @see [[HeapPage]]
    * @return A byte array correspond to the bytes of this page.
    */
  def getPageData: Array[Byte] = {
    val len = BufferPool.PAGE_SIZE
    val baos = new ByteArrayOutputStream(len)
    val dos = new DataOutputStream(baos)

    // create the header of the page
    try {
      header.foreach(b => dos.writeByte(b.toInt))
    } catch {
      case e: IOException =>
        e.printStackTrace()
    }

    // create the tuples
    for (i <- tuples.indices) {
      if (!getSlot(i)) {
        // empty slot
        try {
          (0 until td.getSize).foreach(_ => dos.writeByte(0))
        } catch {
          case e: IOException => e.printStackTrace()
        }
      } else {
        // non-empty slot
        val tuple = tuples(i)
        try {
          (0 until td.numFields).foreach(j => tuple.getField(j))
        } catch {
          case e: IOException => e.printStackTrace()
        }
      }
    }

    // padding
    val zeroLen = BufferPool.PAGE_SIZE - (header.length + td.getSize * tuples.length)
    val zeroes = Array.ofDim[Byte](zeroLen)
    try {
      dos.write(zeroes, 0, zeroLen)
    } catch {
      case e: IOException => e.printStackTrace()
    }

    try {
      dos.flush()
    } catch {
      case e: IOException => e.printStackTrace()
    }

    baos.toByteArray
  }

  /**
    * Delete the specified tuple from the page;  the tuple should be updated to reflect
    *   that it is no longer stored on any page.
    * @throws DbException if this tuple is not on this page, or tuple slot is
    *         already empty.
    * @param t The tuple to delete
    */
  def deleteTuple(t: Tuple): Unit = ???

  /**
    * Adds the specified tuple to the page;  the tuple should be updated to reflect
    *  that it is now stored on this page.
    * @throws DbException if the page is full (no empty slots) or tupledesc
    *         is mismatch.
    * @param t The tuple to add.
    */
  def addTuple(t: Tuple): Unit = ???

  /**
    * Marks this page as dirty/not dirty and record that transaction
    * that did the dirtying
    */
  def markDirty(dirty: Boolean, tid: TransactionId): Unit = ???

  /**
    * Returns the tid of the transaction that last dirtied this page, or null if the page is not dirty
    */
  def isDirty: TransactionId = ???

  /**
    * Returns the number of empty slots on this page.
    */
  def getNumEmptySlots: Int = ???

  /**
    * Returns true if associated slot on this page is filled.
    */
  def getSlot(i: Int): Boolean = ???

  /**
    * Abstraction to fill or clear a slot on this page.
    */
  def setSlot(i: Int, value: Boolean): Unit = ???

  /**
    * @return an iterator over all tuples on this page (calling remove on this iterator throws an UnsupportedOperationException)
    * (note that this iterator shouldn't return tuples in empty slots!)
    */
  def iterator: Iterator[Tuple] = ???
}

object HeapPage {
  /**
    * Static method to generate a byte array corresponding to an empty
    * HeapPage.
    * Used to add new, empty pages to the file. Passing the results of
    * this method to the HeapPage constructor will create a HeapPage with
    * no valid tuples in it.
    *
    * @return The returned ByteArray.
    */
  def createEmptyPageData(): Array[Byte] = Array.ofDim[Byte](BufferPool.PAGE_SIZE)
}
