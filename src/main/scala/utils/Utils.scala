package org.amnairfan.hw3

import java.time.{LocalDate, Period}
import java.util.Date
import java.time.format.DateTimeFormatter
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.reflect.ClassTag
import scala.util.Random

/*
* This class is a collection of helper methods that the simulator uses.
*
* @author Amna Irfan
*/

object Utils {

  //transforms the stock losses to positive values
  def normalizeLosses(stockLosses: Array[(String, Double)]): Array[(String, Double)] = {
      val min = stockLosses.map((s) => {
        (s._2)
      }).reduce((a, b) => {
        (Math.min(a, b))
      })
    if (min > 0) {
      return (stockLosses)
    }
    (stockLosses.map((s) => {(s._1, s._2 + math.abs(min)+1)}))
  }

  // calculates the average loss between a set of random dates of a specific stock
  def calculateAvgStockLoss(randomStocks: Array[(Date, Double)], targetStock: (Date, Double)): Double = {
    val d2 = targetStock._1
    val loss = randomStocks.map((s) => {
      val d1 = s._1
      if (d1.before(d2)) {
        s._2 - targetStock._2
      } else {
        targetStock._2 - s._2
      }
    }).reduce((a1, a2) => {
      a1 + a2
    })

    loss/randomStocks.length
  }

  //converts a string to a java date
  def convertStringToDate(s: String): Date = {
    val format = new java.text.SimpleDateFormat(DateFormat.YYYYMMDD)
    (format.parse(s))
  }

  //gets random sample from a generic array. This is used in getting random stock sample
  def takeSample[T:ClassTag](a:Array[T],n:Int,seed:Long) = {
    val rnd = new Random(seed)
    Array.fill(n)(a(rnd.nextInt(a.size)))
  }

  //gets business days between two days. The days that are observed by the stock market as holidays are also removed.
  def getDatesInBetween(startDate: Date, endDate: Date): ListBuffer[Date] = {
    val df = DateTimeFormatter.ofPattern(DateFormat.EEEMMMDD)
    val days = Period.between(LocalDate.parse(startDate.toString, df), LocalDate.parse(endDate.toString, df)).getDays
    var dates = new ListBuffer[Date]()
    val filteredDays = new ListBuffer[Date]()
    0 to days foreach { i => ({
      if (i == 0 ) {
        dates += startDate
      } else {
        dates += new Date(dates(i-1).getTime + Date.DAY)
      }
    })}
    dates.foreach((d) => {
      val weekDay = LocalDate.parse(d.toString, df).getDayOfWeek
      if (weekDay.getValue != WeekDay.SATURDAY && weekDay.getValue != WeekDay.SUNDAY && !isDateHoliday(LocalDate.parse(d.toString, df))) {
        filteredDays += d
      }
    })
    (filteredDays)
  }

  //gets item by key. This is used to get the stats on each stock
  def getItemByKey(items:Array[(String, Double)], key:String): Double = {
    items.foreach((i) => {
      if (i._1 == key) {
        return i._2
      }
    })
    0
  }

  //gets items by key. This is used to get percentile stats on each stock
  def getItemsByKey(items:Array[(String, Array[Double])], key:String): Array[Double] = {
    items.foreach((i) => {
      if (i._1 == key) {
        return i._2
      }
    })
    null
  }

  //finds out whether the date is an observed holiday. These dates only correspond to the last two years
  def isDateHoliday(d:LocalDate): Boolean = {
    val dates  = Array(
      LocalDate.parse("2017-11-23"),
      LocalDate.parse("2017-12-25"),
      LocalDate.parse("2018-01-01"),
      LocalDate.parse("2018-01-15"),
      LocalDate.parse("2018-02-19"),
      LocalDate.parse("2018-03-30"),
      LocalDate.parse("2018-05-28"),
      LocalDate.parse("2018-07-04"),
      LocalDate.parse("2018-09-03"),
      LocalDate.parse("2018-11-22"),
      LocalDate.parse("2018-12-25"),
      LocalDate.parse("2019-01-01"),
      LocalDate.parse("2019-01-21"),
      LocalDate.parse("2019-02-18"),
      LocalDate.parse("2019-04-19"),
      LocalDate.parse("2019-05-27"),
      LocalDate.parse("2019-07-04"),
      LocalDate.parse("2019-09-02"),
      LocalDate.parse("2018-12-05")
    )
    dates.foreach((h) => {
      if (h.compareTo(d) == 0) {
        return true
      }
    })
    (false)
  }
}
