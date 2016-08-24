package simpledb

import java.io._

import simpledb.Predicate.OP.OP

/**
  * Trait for values of fields in tuples in SimpleDB.
  */
trait Field {
  /**
    * Write the bytes representing this field to the specified
    * DataOutputStream.
    * @see DataOutputStream
    * @param dos The DataOutputStream to write to.
    */
  def serialize(dos: DataOutputStream): Unit

  /**
    * Compare the value of this field object to the passed in value.
    * @param op The operator
    * @param value The value to compare this Field to
    * @return Whether or not the comparison yields true.
    */
  def compare(op: OP, value: Field): Boolean

  /**
    * Returns the type of this field (see [[Type.IntType]] or [[Type.StringType]]
    * @return type of this field
    */
  def getType: Type

  /**
    * Hash code.
    * Different Field objects representing the same value should probably
    * return the same hashCode.
    */
  def hashCode(): Int

  def equals(obj: scala.Any): Boolean

  def toString: String
}
