package simpledb

import simpledb.Predicate.OP.OP

/** Predicate compares tuples to a specified Field value.
  *
  * @constructor Constructor.
  * @param field field number of passed in tuples to compare against.
  * @param op operation to use for comparison.
  * @param operand field value to compare passed in tuples to.
  */
class Predicate(val field: Int, val op: OP, operand: Field) {
  // TODO

  /** Compares the field number of t specified in the constructor to the
    * operand field specified in the constructor using the operator specific
    * in the constructor.  The comparison can be made through Field's
    * compare method.
    *
    * @param t The tuple to compare against
    * @return true if the comparison is true, false otherwise.
    */
  def filter(t: Tuple): Boolean = {
    // TODO
    false
  }

  /** Returns something useful, like
    * "f = field_id op = op_string operand = operand_string
    */
  override def toString: String = {
    // TODO
    ""
  }
}

object Predicate {
  /** Constants used for return codes in Fields.compare */
  object OP extends Enumeration {
    type OP = Value
    val EQUALS, GREATER_THAN, LESS_THAN, LESS_THAN_OR_EQ, GREATER_THAN_OR_EQ, LIKE, NOT_EQUALS = Value
  }

  /** Interface to access operations by a string containing an integer
    * index for command-line convenience.
    *
    * @param s a string containing a valid integer Op index
    */
  def getOp(s: String) = OP.withName(s)

  /** Interface to access operations by integer value for command-line
    * convenience.
    *
    * @param i a valid integer Op index
    */
  def getOp(i: Int) = OP(i)
}
