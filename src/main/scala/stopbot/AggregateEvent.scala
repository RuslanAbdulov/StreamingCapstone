package stopbot

case class AggregateEvent(amount: BigInt,
                          window: String,
                          ipAddress: String,
                          rate: String,
                          categories: String) {
}
