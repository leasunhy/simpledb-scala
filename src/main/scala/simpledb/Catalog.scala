package simpledb

import java.io.BufferedReader
import java.io.File
import java.io.FileReader
import java.io.IOException

import scala.io.Source

/**
  * The Catalog keeps track of all available tables in the database and their
  * associated schemas.
  * For now, this is a stub catalog that must be populated with tables by a
  * user program before it can be used -- eventually, this should be converted
  * to a catalog that reads a catalog table from disk.
  *
  * @constructor Creates a new, empty catalog.
  */
class Catalog {
  /**
    * Add a new table to the catalog.
    * This table's contents are stored in the specified DbFile.
    * @param file the contents of the table to add;  file.getId() is the identfier of
    *    this file/tupledesc param for the calls getTupleDesc and getFile
    * @param name the name of the table -- may be an empty string.  May not be null.  If a name
    * @param pkeyField the name of the primary key field
    * conflict exists, use the last table to be added as the table for a given name.
    */
  def addTable(file: DbFile, name: String, pkeyField: String = "") = ???

  /**
    * Return the id of the table with a specified name,
    * @throws NoSuchElementException if the table doesn't exist
    */
  def getTableId(name: String): Int = ???

  /**
    * Returns the tuple descriptor (schema) of the specified table
    * @param tableId The id of the table, as specified by the DbFile.getId()
    *     function passed to addTable
    */
  def getTupleDesc(tableId: Int): TupleDesc = ???

  /**
    * Returns the DbFile that can be used to read the contents of the
    * specified table.
    * @param tableId The id of the table, as specified by the DbFile.getId()
    *     function passed to addTable
    */
  def getDbFile(tableId: Int): DbFile = ???

  /** Delete all tables from the catalog. */
  def clear(): Unit = ???

  /** Get the primary key for table with id = tableId */
  def getPrimaryKey(tableId: Int): String = ???

  /** Get the iterator of table ids. */
  def tableIdIterator(): Iterator[Int] = ???

  /** Get the name of table with id = tableId. */
  def getTableName(id: Int) = ???

  /**
    * Reads the schema from a file and creates the appropriate tables in the database.
    * @param catalogFile
    */
  def loadSchema(catalogFile: String): Unit = {
    for (line <- Source.fromFile(catalogFile).getLines) {
      try {
        val name = line.substring(0, line.indexOf("(")).trim
        val fields = line.substring(line.indexOf("(") + 1, line.indexOf(")")).trim
        val els = fields.split(",")
        val names = scala.collection.mutable.ArrayBuffer.empty[String]
        val types = scala.collection.mutable.ArrayBuffer.empty[Type]
        var primaryKey = ""
        for (e <- els) {
          val els2 = e.trim.split(" ")
          names += els2(0).trim
          els2(1).trim.toLowerCase match {
            case "Int" => types += Type.IntType
            case "String" => types += Type.StringType
            case _ => System.out.println(s"Unknown type ${els(2)}")
          }
          if (els2.length == 3) {
            if (els2(2).trim.equals("pk")) {
              primaryKey = els2(0).trim
            } else {
              println(s"Unknown annotation: ${els2(2)}")
              System.exit(0)
            }
          }
        }
        val t = new TupleDesc(types.toArray, names.toArray)
        val tabHf = new HeapFile(new File(name + ".dat"), t)
        addTable(tabHf, name, primaryKey)
        println("Added table : " + name + " with schema " + t)
      } catch {
        case e: IOException =>
          e.printStackTrace()
          System.exit(0)
        case e: IndexOutOfBoundsException =>
          println("Invalid catalog entry : " + line)
      }
    }
  }
}
