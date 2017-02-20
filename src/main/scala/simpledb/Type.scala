package simpledb

import java.io._
import java.text.ParseException

abstract class Type {
  /**
    * @return the number of bytes required to store a field of this type.
    */
  def getLen: Int

  /**
    * @return a Field object of the same type as this object that has contents
    *   read from the specified DataInputStream.
    * @param dis The input stream to read from
    * @throws ParseException if the data read from the input stream is not
    *   of the appropriate type.
    */
  def parse(dis: DataInputStream): Field
}

object Type {
  /**
    * Represents a type of Int.
    */
  object IntType extends Type {
    /**
      * @return the number of bytes required to store a field of this type.
      */
    override def getLen: Int = 4

    /**
      * @return a Field object of the same type as this object that has contents
      *         read from the specified DataInputStream.
      * @param dis The input stream to read from
      * @throws ParseException if the data read from the input stream is not
      *                        of the appropriate type.
      */
    override def parse(dis: DataInputStream): Field = {
      try {
        new IntField(dis.readInt())
      } catch {
        case _: IOException => throw new ParseException("Couldn't parse", 0)
      }
    }

    override def equals(obj: scala.Any): Boolean = obj.asInstanceOf[Type].hashCode() == hashCode()

    override def hashCode(): Int = 1
  }

  /**
    * Represents a type of string.
    */
  object StringType extends Type {
    final val STRING_LEN: Int = 128

    /**
      * @return the number of bytes required to store a field of this type.
      */
    override def getLen: Int = STRING_LEN + 4

    /**
      * @return a Field object of the same type as this object that has contents
      *         read from the specified DataInputStream.
      * @param dis The input stream to read from
      * @throws ParseException if the data read from the input stream is not
      *                        of the appropriate type.
      */
    override def parse(dis: DataInputStream): Field = {
      try {
        val strLen = dis.readInt()
        val bs = Array.ofDim[Byte](strLen)
        dis.read(bs)
        dis.skipBytes(STRING_LEN - strLen)
        new StringField(new String(bs), STRING_LEN)
      } catch {
        case _: IOException => throw new ParseException("Couldn't parse", 0)
      }
    }

    override def equals(obj: scala.Any): Boolean = obj.asInstanceOf[Type].hashCode() == hashCode()

    override def hashCode(): Int = 2
  }
}

