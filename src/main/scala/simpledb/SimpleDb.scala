package simpledb

import java.io.{File, IOException}

object SimpleDb {
  def main(args: Array[String]): Unit = {
    // convert a file
    args(0) match {
      case "convert" =>
        try {
          if (args.length == 3) {
            HeapFileEncoder.convert(new File(args(1)),
              new File(args(1).replaceAll(".txt", ".dat")),
              BufferPool.PAGE_SIZE,
              Integer.parseInt(args(2)))
          } else if (args.length == 4) {
            val typeStringAr = args(3).split(',')
            val ts = typeStringAr.map {
              case "int" => Type.IntType
              case "string" => Type.StringType
              case s =>
                System.out.println(s"Unknown type $s")
                System.exit(0)
                null
            }
            HeapFileEncoder.convert(new File(args(1)),
              new File(args(1).replaceAll(".txt", ".dat")),
              BufferPool.PAGE_SIZE,
              Integer.parseInt(args(2)), ts)
          } else {
            System.out.println("Unexpected number of arguments to convert ")
          }
        } catch {
          case e: IOException =>
            throw new RuntimeException(e)
        }
      case "print" =>
        val tableFile = new File(args(1))
        val columns = args(2).toInt
        val table = Utility.openHeapFile(columns, tableFile)
        val tid = new TransactionId()
        val it = table.iterator(tid)

        if (it == null) {
          System.out.println("Error: method HeapFile.iterator(TransactionId tid) not yet implemented!")
        } else {
          it.open()
          while (it.hasNext())
            System.out.println(it.next)
          it.close()
        }
      case "parser" =>
        // Strip the first argument and call the parser
        val newArgs = Array.tabulate(args.length - 1)(i => args(i + 1))

        try {
          // dynamically load Parser -- if it doesn't exist, print error message
          val c = Class.forName("simpledb.Parser")
          val s = classOf[Array[String]]
          val m = c.getMethod("main", s)
          m.invoke(null, newArgs)
        } catch {
          case _: Exception =>
            System.out.println("Class Parser not found -- perhaps you are trying to run the parser as a part of lab1?")
        }
      case _ =>
        System.err.println("Unknown command: " + args(0))
        System.exit(1)
    }
  }
}
