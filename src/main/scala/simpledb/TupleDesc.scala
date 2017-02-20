package simpledb

import java.util._

/**
  * TupleDesc describes the schema of a tuple.
  *
  * @constructor Create a new TupleDesc with types.length fields with fields of the specified
  * types, with associated named fields.
  * @param types array specifying the number of and types of fields in
  *        this TupleDesc. It must contain at least one entry.
  * @param fields array specifying the names of the fields. Note that names may be null.
  */
class TupleDesc(val types: Array[Type], val fields: Array[String]) {
  // TODO

  /**
    * Constructor.
    * Create a new tuple desc with typeAr.length fields with fields of the
    * specified types, with anonymous (unnamed) fields.
    *
    * @param types array specifying the number of and types of fields in
    *        this TupleDesc. It must contain at least one entry.
    */
  def this(types: Array[Type]) = this(types, null)

  /**
    * @return the number of fields in this TupleDesc
    */
  val numFields: Int = types.length

  /**
    * Gets the (possibly null) field name of the ith field of this TupleDesc.
    *
    * @param i index of the field name to return. It must be a valid index.
    * @return the name of the ith field
    * @throws NoSuchElementException if i is not a valid field reference.
    */
  def getFieldName(i: Int): String = if (fields != null) fields(i) else null

  /**
    * Find the index of the field with a given name.
    *
    * @param name name of the field.
    * @return the index of the field that is first to have the given name.
    * @throws NoSuchElementException if no field with a matching name is found.
    */
  def nameToId(name: String): Int = fields.indexOf(name) match {
    case -1 => throw new NoSuchElementException()
    case i => i
  }

  /**
    * Gets the type of the ith field of this TupleDesc.
    *
    * @param i The index of the field to get the type of. It must be a valid index.
    * @return the type of the ith field
    * @throws NoSuchElementException if i is not a valid field reference.
    */
  def getType(i: Int): Type = types(i)

  /**
    * @return The size (in bytes) of tuples corresponding to this TupleDesc.
    * Note that tuples from a given TupleDesc are of a fixed size.
    */
  def getSize: Int = types.map(_.getLen).sum

  /**
    * Compares the specified object with this TupleDesc for equality.
    * Two TupleDescs are considered equal if they are the same size and if the
    * n-th type in this TupleDesc is equal to the n-th type in td.
    *
    * @param o the Object to be compared for equality with this TupleDesc.
    * @return true if the object is equal to this TupleDesc.
    */
  override def equals(o: scala.Any): Boolean = o match {
    case null => false
    case otd: TupleDesc => getSize == otd.getSize && (types, otd.types).zipped.forall{ case (t1, t2) => t1 == t2 }
    case _ => false
  }

  /* If you want to use TypleDesc as keys for HashMap,
     implement this so that equal objects have equal hash codes */
  override def hashCode(): Int = ???

  /**
    * Returns a String describing this descriptor. It should be of the form
    * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
    * the exact format does not matter.
    * @return String describing this descriptor.
    */
  override val toString: String = fields match {
    case null => types.mkString(", ")
    case _ => (types, fields).zipped.map{ case (t, n) => s"${t}(${n})" }.mkString(", ")
  }
}

object TupleDesc {
  /**
    * Merge two TupleDescs into one, with td1.numFields + td2.numFields
    * fields, with the first td1.numFields coming from td1 and the remaining
    * from td2.
    * @param td1 The TupleDesc with the first fields of the new TupleDesc
    * @param td2 The TupleDesc with the last fields of the TupleDesc
    * @return the new TupleDesc
    */
  def combine(td1: TupleDesc, td2: TupleDesc): TupleDesc =
    new TupleDesc(td1.types ++ td2.types, td1.fields ++ td2.fields)

}
