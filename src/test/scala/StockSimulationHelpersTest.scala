package org.amnairfan.hw3

import java.time.LocalDate
import java.util.Date
import java.text.DateFormat
import java.text.SimpleDateFormat

import com.typesafe.scalalogging.LazyLogging
import org.scalatest.{BeforeAndAfterEach, FunSuite}

/*
* This class consists of eight unit tests. Each unit test corresponds to a method defined in the util class
* of the utils package.
*
* @author Amna Irfan
*/

class StockSimulationHelpersTest extends FunSuite with BeforeAndAfterEach with LazyLogging  {

  test("Utility.normalizeLosses\n") {
    logger.info("Verifying the loss normalization method\n")
    val losses = Array(("AAPL", -3.0), ("RHI", -9.0), ("HPQ", 3.5), ("DIS", 9.5))
    val normalized = Utils.normalizeLosses(losses)
    assert(normalized(0)._2 == 7.0 && normalized(1)._2 == 1.0 && normalized(2)._2 == 13.5 && normalized(3)._2 == 19.5)
  }

  test("Utility.calculateAvgStockLoss\n") {
    logger.info("Verifying the average stock loss calculation\n")
    val dateFormat = new SimpleDateFormat(DateFormat.YYYYMMDD)
    val neighbors = Array((dateFormat.parse("2018-10-19"), 9.0), (dateFormat.parse("2018-10-18"), 7.0), (dateFormat.parse("2018-10-21"), 6.0), (dateFormat.parse("2018-10-21"), 5.0))
    val target = (dateFormat.parse("2018-10-20"), 3.5)
    assert(Utils.calculateAvgStockLoss(neighbors, target) == 1.25)
  }

  test("Utility.convertStringToDate\n") {
    logger.info("Verifying the convertStringToDate method of util\n")
    val d = Utils.convertStringToDate("2019-08-19")
    assert(d.getTime/1000 == 1566190800)
  }

  test("Utility.takeSample\n") {
    logger.info("Verifying the takeSample method of util\n")
    val list = Array("apple", "banana", "peach", "oranges")
    val sample = Utils.takeSample(list, 2, System.currentTimeMillis())
    assert(sample.length == 2 )
  }

  test("Utility.getDatesInBetween\n") {
    logger.info("Verifying getDatesInBetween method of util\n")
    val dates = Utils.getDatesInBetween(Utils.convertStringToDate("2019-10-01"), Utils.convertStringToDate("2019-10-31"))
    assert(dates.length == 23)
  }

  test("Utility.getItemByKey\n") {
    logger.info("Verifying the getItemByKey method of util\n")
    val items = Array(("AAPL", 3.0), ("RHI", 9.0), ("HPQ", 3.5), ("DIS", 9.5))
    val item = Utils.getItemByKey(items, "DIS")
    assert(item == 9.5)
  }

  test("Utility.getItemsByKey\n") {
    logger.info("Verifying the getItemByKey method of util\n")
    val items = Array(("AAPL", Array(3.0, 9.5)), ("RHI", Array(7.0, 3.5)), ("HPQ", Array(3.0, 2.5)), ("DIS", Array(9.5, 1.0)))
    val item = Utils.getItemsByKey(items, "DIS")
    assert(item.length == 2 && item(0) == 9.5 && item(1) == 1.0)
  }

  test("Utility.isDateHoliday\n") {
    logger.info("Verifying the isDateHoliday method of util\n")
    val isChristmasHoliday = Utils.isDateHoliday(LocalDate.parse("2018-12-25"))
    val isNewYearsHoliday = Utils.isDateHoliday(LocalDate.parse("2019-01-01"))
    val isRandomDayHoliday = Utils.isDateHoliday(LocalDate.parse("2018-10-03"))
    assert(isChristmasHoliday && isNewYearsHoliday && !isRandomDayHoliday)
  }
}
