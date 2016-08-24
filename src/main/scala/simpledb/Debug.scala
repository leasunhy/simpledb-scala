package simpledb

object Debug {
  private final val DEBUG_LEVEL: Int = System.getProperty("simpledb.Debug") match {
    // No system property = disabled
    case null => -1
    // Empty property = level 0
    case ""   =>  0
    case s    => Integer.parseInt(s)
  }

  private final val DEFAULT_LEVEL: Int = 0

  /** Log message if the log level >= level. Uses printf. */
  def log(level: Int, message: String, args: Any*): Unit = {
    if (isEnabled(level)) {
      System.out.printf(message, args)
      System.out.println()
    }
  }

  /** @return true if level is being logged. */
  def isEnabled(level: Int): Boolean = level <= DEBUG_LEVEL

  /** @return true if the default level is being logged */
  def isEnabled(): Boolean = isEnabled(DEFAULT_LEVEL)

  /** Logs message at the default log level. */
  def log(message: String, args: Any*): Unit = log(DEFAULT_LEVEL, message, args)
}
