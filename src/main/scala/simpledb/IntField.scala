package simpledb

import java.io.DataOutputStream

import simpledb.Predicate.OP
import simpledb.Predicate.OP.OP

/**
  * Instance of Field that stores a single integer.
  *
  * @constructor Constructor.
  * @param value The value of this field.
  */
class IntField(val value: Int) extends Field {
  override def toString: String = value.toString

  override def hashCode(): Int = value

  override def equals(obj: scala.Any): Boolean = obj.asInstanceOf[IntField].value == value

  /**
    * Returns the type of this field (see [[Type.IntType]] or [[Type.StringType]]
    *
    * @return [[Type.IntType]]
    */
  override def getType: Type = Type.IntType

  /**
    * Write the bytes representing this field to the specified
    * DataOutputStream.
    *
    * @see DataOutputStream
    * @param dos The DataOutputStream to write to.
    */
  override def serialize(dos: DataOutputStream): Unit = dos.writeInt(value)

  /**
    * Compare the value of this field object to the passed in value.
    *
    * @param op    The operator
    * @param other The value to compare this Field to
    * @return Whether or not the comparison yields true.
    */
  override def compare(op: OP, other: Field): Boolean = {
    val intOther = other.asInstanceOf[IntField]
    op match {
      case OP.LIKE               => value == intOther.value
      case OP.EQUALS             => value == intOther.value
      case OP.NOT_EQUALS         => value != intOther.value
      case OP.GREATER_THAN       => value >  intOther.value
      case OP.LESS_THAN          => value <  intOther.value
      case OP.GREATER_THAN_OR_EQ => value >= intOther.value
      case OP.LESS_THAN_OR_EQ    => value <= intOther.value
    }
  }

}
