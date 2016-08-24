package simpledb

/**
  * Tuple maintains information about the contents of a tuple.
  * Tuples have a specified schema specified by a TupleDesc object and contain
  * Field objects with the data for each field.
  *
  * @constructor Create a new tuple with the specified schema (type).
  * @param td the schema of this tuple. It must be a valid TupleDesc
  * instance with at least one field.
  */
class Tuple(td: TupleDesc) {
  // TODO
  ???

  /**
    * @return The TupleDesc representing the schema of this tuple.
    */
  def getTupleDesc(): TupleDesc = ???

  /**
    * @return The RecordId representing the location of this tuple on
    *   disk. May be null.
    */
  def getRecordId(): RecordId = ???

  /**
    * Set the RecordId information for this tuple.
    * @param rid the new RecordId for this tuple.
    */
  def setRecordId(rid: RecordId): Unit = ???

  /**
    * Change the value of the ith field of this tuple.
    *
    * @param i index of the field to change. It must be a valid index.
    * @param f new value for the field.
    */
  def setField(i: Int, f: Field): Unit = ???

  /**
    * @return the value of the ith field, or null if it has not been set.
    *
    * @param i field index to return. Must be a valid index.
    */
  def getField(i: Int): Field = ???

  /**
    * Returns the contents of this Tuple as a string.
    * Note that to pass the system tests, the format needs to be as
    * follows:
    *
    * column1\tcolumn2\tcolumn3\t...\tcolumnN\n
    *
    * where \t is any whitespace, except newline, and \n is a newline
    */
  override def toString: String = ???
}
