package stopbot

case class Event(val unixTime: String, val categoryId: String, val ipAddress: String, val eventType: String) {
}
