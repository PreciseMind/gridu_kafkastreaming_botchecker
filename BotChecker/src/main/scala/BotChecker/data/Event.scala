package BotChecker.data

class Event (kindValue: String, ipValue: String, categoryValue: String, unixTimeValue: String) extends Serializable {

  val kind:String = kindValue
  val ip: String = ipValue
  val category: String = categoryValue
  val unixTime: String = unixTimeValue

  override def toString = {
    kind+", "+ ip + ", " + category+ ", " + unixTime
  }
}
