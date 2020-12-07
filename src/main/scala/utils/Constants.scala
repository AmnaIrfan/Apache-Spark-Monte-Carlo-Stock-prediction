package org.amnairfan.hw3

object FileType {
  val CSV = "csv"
}

object FileOption{
  val HEADER = "header"
  val INFERSCHEMA = "inferSchema"
}

object SparkConf {
  val JOBNAME = "MonteCarloStockSimulation"
  val MASTER = "spark.master"
  val LOCAL = "local[1]"
}

object DateFormat {
  val YYYYMMDD = "yyyy-MM-dd"
  val EEEMMMDD  = "EEE MMM dd HH:mm:ss zzz yyyy"
}

object Date {
  val DAY: Int = 3600000 * 24
}

object DataLimit {
  val StartDate = "2018-11-16"
  val EndDate = "2019-11-15"
  val StockCount = 477
  val DayCount = 250
}

object StockColumn {
  val TIMESTAMP = "timestamp"
  val SYMBOL = "symbol"
  val CLOSE = "close"
}

object WeekDay {
  val SATURDAY = 6
  val SUNDAY = 7
}