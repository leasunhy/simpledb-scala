package simpledb

import java.util.concurrent.atomic.AtomicLong

/**
  * TransactionId is a class that contains the identifier of a transaction.
  */
class TransactionId {
  val id = TransactionId.counter.getAndIncrement()

  override def equals(obj: scala.Any): Boolean = obj.asInstanceOf[TransactionId].id == id

  override def hashCode(): Int = id.toInt
}

object TransactionId {
  final val counter = new AtomicLong(0)
}
