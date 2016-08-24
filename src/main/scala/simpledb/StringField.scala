package simpledb

import java.io.DataOutputStream
import java.lang.ClassCastException

import simpledb.Predicate.OP
import simpledb.Predicate.OP.OP

/**
  * Instance of Field that stores a single String of a fixed length.
  *
  * @constructor Constructor.
  * @param oriValue The value of this field, which may be subject to truncation.
  * @param maxSize The maximum size of this string.
  */
class StringField(oriValue: String, maxSize: Int) extends Field {
  val value = if (oriValue.length > maxSize) oriValue.substring(0, maxSize) else oriValue

  override def hashCode(): Int = value.hashCode

  override def toString: String = value

  override def equals(obj: scala.Any): Boolean = obj.asInstanceOf[StringField].value.equals(value)

  /**
    * Returns the type of this field (see [[Type.IntType]] or [[Type.StringType]]
    *
    * @return [[Type.StringType]]
    */
  override def getType: Type = Type.StringType

  /** Write this string to dos.  Always writes maxSize + 4 bytes to the passed in dos.
    * First four bytes are string length, next bytes are string, with remainder padded with 0 to maxSize.
    *
    * @param dos Where the string is written
    */
  override def serialize(dos: DataOutputStream): Unit = {
    dos.writeInt(value.length)
    dos.writeBytes(value)
    var overflow = maxSize - value.length
    while (overflow > 0) {
      dos.write(0)
      overflow -= 1
    }
  }

  /**
    * Compare the specified field to the value of this Field.
    * Return semantics are as specified by Field.compare
    *
    * @throws ClassCastException if val is not a StringField
    * @see [[Field.compare]]
    */
  override def compare(op: OP, other: Field): Boolean = {
    val stringOther = other.asInstanceOf[StringField]
    val cmpVal = value.compareTo(stringOther.value)
    op match {
      case OP.LIKE               => value.indexOf(stringOther.value) >= 0
      case OP.EQUALS             => cmpVal == 0
      case OP.NOT_EQUALS         => cmpVal != 0
      case OP.GREATER_THAN       => cmpVal >  0
      case OP.LESS_THAN          => cmpVal <  0
      case OP.GREATER_THAN_OR_EQ => cmpVal >= 0
      case OP.LESS_THAN_OR_EQ    => cmpVal <= 0
    }
  }
}
