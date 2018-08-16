package utils.stringutils

/**
  * Following object contains the definitions of
  * all the methods related to Date and Time.
  */
object dateTime {
  /**
    * The input string is split and then the day value and
    * the month are converted into corresponding year value
    * (for.ex: 4th month has value of 0.25 years)
    * @param d String specifying the date.
    * @return Double specifying the value of the date.
    */
  def dateValue(d : String) : Double = {
    // Splitting the date into an array based on "/"
    val k = d.split("/")

    // Converting all the integers into float for decimal precision.
    val l = k.map(_.toFloat)

    // Calculating the value based on the month.
    if(l(0) == 2)
      l(0)/365+l(1)/28+l(2)
    else if(l(1) == 1 || l(1) == 3 || l(1) == 5 || l(1) == 7 || l(1) == 8 || l(1) == 10 || l(1) == 12)
      l(0)/365+l(1)/31+l(2)
    else
      l(0)/365+l(1)/30+l(2)
  }

  /**
    * The date string is split and the day value is used to
    * classify the date into one of the values 0,1,2.
    * @param d String specifying the date.
    * @return Integer that has value 0 or 1 or 2.
    */
  def splitDayOfMonth(d: String) : Int = {
    // Splitting the date into an array based on "/"
    val l = d.split("/")

    // Converting the day value into integer.
    val k : Int= l(1).toInt

    // Checking on which 1/3rd part does the day falls on.
    if(k < 11 && k > 0)
      0
    else if(k > 10 && k < 20)
      1
    else
      2
  }

  /**
    * The date string is split and the day value is used to
    * classify the date into one of the values 0,1,2.
    * @param d String specifying the date.
    * @return Integer that has value 0 or 1 or 2.
    */
  def splitDayOfWeek(d : String) : Int = {
    // Splitting the date into an array based on "/"
    val l = d.split("/")

    // Using datetoday for finding which day of the week is the given date.
    val day : Int = dateToDay(l(1).toInt,l(0).toInt,l(2).toInt)

    // Classifying the day into 3 different blocks.
    if(day == 1 || day == 2){
      0
    }
    else if (day == 0 || day == 6){
      2
    }
    else {
      1
    }
  }

  /**
    * The time string is split and the hour value is used to
    * classify the time into one of the values 0,1,2,3.
    * @param t String specifying the time.
    * @return Integer that has value 0 or 1 or 2 or 3.
    */
  def splitTime(t : String) : Int = {
    // Splitting the date into an array based on ":"
    val l = t.split(":")

    // Converting the hour value to integer.
    val k : Int = l(0).toInt

    // Classifying based on the period of day.
    if(k <= 9 && k >= 3)
      0
    else if (k >= 9 && k <= 15)
      1
    else if (k >= 15 && k <= 21)
      2
    else
      3
  }

  /**
    * Converting date to day of the week.
    * @param day Integer that specifies the day, value between 1 and 31(ends inclusive).
    * @param month Integer that specifies which month, between 1 and 12(ends inclusive).
    * @param year Integer that specifies which year.
    * @return an integer that ranges from 0 to 6.
    */
  def dateToDay(day:Int,month:Int,year:Int) : Int = {
    var z = 0
    if(month < 3){
      z = year - 1
    }
    else{
      z = year
    }
    var dayOfWeek : Int =  23*month/9 + day + 4 + year + z/4 - z/100 + z/400
    dayOfWeek = dayOfWeek%7
    dayOfWeek
  }

}
