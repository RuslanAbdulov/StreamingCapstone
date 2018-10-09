package stopbot

case class Event(unixTime: Long, categoryId: Int, ipAddress: String, eventType: String) {
  def isClick: Boolean = eventType == "click"
  def isView: Boolean = eventType == "view"
}

//abstract class EventAbstract {
//  def unixTime: Long
//  def categoryId: Int
//  def ipAddress: String
//  def eventType: String
//}

//case class Click(unixTime: Long, categoryId: Int, ipAddress: String, eventType: String) extends EventAbstract
//case class View(unixTime: Long, categoryId: Int, ipAddress: String, eventType: String) extends EventAbstract