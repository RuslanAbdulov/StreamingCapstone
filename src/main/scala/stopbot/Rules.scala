package stopbot


object Rules {

  //Enormous event rate, e.g. more than 1000 request in 10 minutes*.
  def enormousRate(): Unit = {}

  //High difference between click and view events, e.g. (clicks/views) more than 3-5. Correctly process cases when there is no views.
  def highDifference(): Unit = {}

  //Looking for many categories during the period, e.g. more than 5 categories in 10 minutes.
  def manyCategories(): Unit = {}

}
