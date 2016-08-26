package simpledb

import java.io._

import scala.io.Source

/**
  * HeapFileEncoder reads a comma delimited text file or accepts
  * an array of tuples and converts it to
  * pages of binary data in the appropriate format for simpledb heap pages
  * Pages are padded out to a specified length, and written consecutive in a
  * data file.
  */
object HeapFileEncoder {

  /** Convert the specified tuple list (with only integer fields) into a binary
    * page file. <br>
    *
    * The format of the output file will be as specified in HeapPage and
    * HeapFile.
    *
    * @see HeapPage
    * @see HeapFile
    * @param tuples the tuples - a list of tuples, each represented by a list of integers that are
    *        the field values for that tuple.
    * @param outFile The output file to write data to
    * @param nPageBytes The number of bytes per page in the output file
    * @param numFields the number of fields in each input tuple
    * @throws IOException if the temporary/output file can't be opened
    */
  def convert(tuples: IndexedSeq[IndexedSeq[Integer]], outFile: File, nPageBytes: Int, numFields: Int) {
    val tempInput = File.createTempFile("tempTable", ".txt")
    tempInput.deleteOnExit()
    val bw = new BufferedWriter(new FileWriter(tempInput))
    for (tuple <- tuples) {
      if (tuple.length > numFields)
        throw new RuntimeException(s"Tuple has more than $numFields fields: (${Utility.listToString(tuple)})")
      bw.write(tuple.mkString(","))
      bw.write('\n')
    }
    bw.close()
    convert(tempInput, outFile, nPageBytes, numFields)
  }

  def convert(inFile: File, outFile: File, nPageBytes: Int, numFields: Int) {
    val ts = Array.fill[Type](numFields)(Type.IntType)
    convert(inFile, outFile, nPageBytes, numFields, ts)
  }

  /** Convert the specified tuple list (with only integer fields) into a binary
    * page file. <br>
    *
    * The format of the output file will be as specified in HeapPage and
    * HeapFile.
    *
    * @see HeapPage
    * @see HeapFile
    * @param tuples the tuples - a list of tuples, each represented by a list of integers that are
    *        the field values for that tuple.
    * @param outFile The output file to write data to
    * @param nPageBytes The number of bytes per page in the output file
    * @param numFields the number of fields in each input tuple
    * @throws IOException if the temporary/output file can't be opened
    */
  def convert(inFile: File, outFile: File, nPageBytes: Int, numFields: Int, typeAr: Array[Type]) {
    // TODO: this scala translation of thr original java method may have issues
    val nRecBytes = typeAr.view.map(_.getLen).sum
    val nRecords = (nPageBytes * 8) / (nRecBytes * 8 + 1)  // floor comes for free

    // per record, we need one bit; there are nRecords per page, so we need
    //   nRecords bits, i.e. ((nRecords / 8) + 1) Bytes
    val nHeaderBytes = (nRecords + 7) / 8
    val nHeaderBits = nHeaderBytes * 8

    val br = Source.fromFile(inFile)
    val os = new FileOutputStream(outFile)

    var recordCount = 0
    var nPages = 0

    var headerBAOS = new ByteArrayOutputStream(nHeaderBytes)
    var headerStream = new DataOutputStream(headerBAOS)
    var pageBAOS = new ByteArrayOutputStream(nPageBytes)
    var pageStream = new DataOutputStream(pageBAOS)

    def flush() = {
      var i = 0
      var headerByte = 0.toByte
      while (i < nHeaderBits) {
        if (i < recordCount)
          headerByte |= (1 << (i % 8))
        if ((i + 1) % 8 == 0) {
          headerStream.writeByte(headerByte)
          headerByte = 0
        }
        i += 1
      }

      if (i % 8 > 0)
        headerStream.writeByte(headerByte)

      // pad the rest of the page with zeroes
      i = 0
      while (i < nPageBytes - (recordCount * nRecBytes + nHeaderBytes)) {
        pageStream.writeByte(0)
        i += 1
      }

      // write the header and body to file
      headerStream.flush()
      headerBAOS.writeTo(os)
      pageStream.flush()
      pageBAOS.writeTo(os)

      // reset header and body for next page
      headerBAOS = new ByteArrayOutputStream(nHeaderBytes)
      headerStream = new DataOutputStream(headerBAOS)
      pageBAOS = new ByteArrayOutputStream(nPageBytes)
      pageStream = new DataOutputStream(pageBAOS)

      recordCount = 0
      nPages += 1
    }

    for (line <- br.getLines(); trimmedLine = line.trim; if trimmedLine.length() > 0) {
      recordCount += 1
      val fields = line.split(',')
      (typeAr, fields).zipped.foreach {
        case (Type.IntType, field) =>
          try {
            pageStream.writeInt(field.toInt)
          } catch {
            case NumberFormatException => System.out.println("BAD LINE : " + field)
          }
        case (Type.StringType, field) =>
          val overflow = Type.StringType.STRING_LEN - field.length
          val s = if (overflow < 0) trimmedLine.substring(0, Type.StringType.STRING_LEN) else field
          pageStream.writeInt(s.length)
          pageStream.writeBytes(s)
          (overflow until 0).foreach(_ => pageStream.writeByte(0))
      }

      // if we wrote a full page of records, or if we're done altogether,
      // write out the header of the page.
      //
      // in the header, write a 1 for bits that correspond to records we've
      // written and 0 for empty slots.
      //
      // when we're done, also flush the page to disk, but only if it has
      // records on it.  however, if this file is empty, do flush an empty
      // page to disk.
      if (recordCount >= nRecords)
        flush()
    }

    flush()

    br.close()
    os.close()
  }
}

