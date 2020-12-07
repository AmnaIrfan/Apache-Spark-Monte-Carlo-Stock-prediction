package org.airfan5.hw3

import java.sql.Timestamp
import java.util.Date
import java.text.SimpleDateFormat

import com.typesafe.config.{Config, ConfigFactory}
import org.amnairfan.hw3.{BuyStockPolicy, DataLimit, FileOption, FileType, SellStockPolicy, SparkConf, Stock, StockColumn, Utils}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.{ArrayBuffer, ListBuffer}


/*
* This is the starting point of the simulation. The main method collects information on the the parameters of the simulation through
* the configuration file. We use Apache Spark to create random simulations using Montecarlo that in turn take advantage
* of Sparks RDDS and DataFrames to find the optimal policy for buying and selling stocks.
*
* @author Amna Irfan
*/


object Simulation {

  def main(args: Array[String]): Unit = {

    val logger: Logger = LoggerFactory.getLogger(getClass)
    logger.info("Loading Configuration file.")
    val configuration: Config = ConfigFactory.load("stocksimulation.conf")
    logger.info("Configuration file has been loaded")

    val stockPoolSize = configuration.getInt("simulation.stockPoolSize")
    val startDate = Utils.convertStringToDate(configuration.getString("simulation.startDate"))
    val endDate = Utils.convertStringToDate(configuration.getString("simulation.endDate"))
    val inputDir = configuration.getString("simulation.inputDir")
    val outputDir = configuration.getString("simulation.outputDir")
    val numberOfSimulations = configuration.getInt("simulation.numberOfSimulations")
    val randomLossStockCount = configuration.getInt("simulation.randomLossStockCount")
    val startingFund = configuration.getInt("simulation.startingFund")

    //Make sure the user's request is valid.
    if (startDate.after(endDate)) {
      logger.error("Start date of the simulation is greater than the end date")
      System.exit(0)
    }

    //Make sure that the date range the user has requested is within the data range we have in our training data
    if (endDate.after(Utils.convertStringToDate(DataLimit.EndDate))) {
      logger.error("End date of the simulation does not fit the stock data's date range")
      System.exit(0)
    }

    if (startDate.before(Utils.convertStringToDate(DataLimit.StartDate))) {
      logger.error("Start date of the simulation does not fit the stock data's date range")
      System.exit(0)
    }

    //Make sure the number of requested stocks is not greater than the total amount of unique stocks we have in our data.
    if (stockPoolSize > DataLimit.StockCount) {
      logger.error("The stock pool size is greater than the number of unique stocks we have in the data")
      System.exit(0)
    }

    //Make sure the number of requested stocks is not greater than the total days we have records for each stock
    if (randomLossStockCount > DataLimit.DayCount) {
      logger.error("The stock pool size is greater than the total number of days we have recorded for each stock")
      System.exit(0)
    }


    logger.info("Loading the stock data as a Spark Dataframe.")
    val spark = SparkSession
      .builder()
      .config(SparkConf.MASTER, SparkConf.LOCAL)
      .appName(SparkConf.JOBNAME)
      .getOrCreate()

    val stockDf = spark.
      read.
      format(FileType.CSV).
      option(FileOption.HEADER, true).
      option(FileOption.INFERSCHEMA, true).
      csv(inputDir)


    //Get the list of stock names in the data
    val stockList = stockDf.select(stockDf.col(StockColumn.SYMBOL)).distinct().rdd.map((s) => {
      (s.getString(0))
    }).collect()
    val randomStockList = Utils.takeSample(stockList, stockPoolSize, System.currentTimeMillis);

    //Filter data-frame to only contain the stocks in the random stock list
    val filteredByNameStockDf = stockDf.filter(stockDf.col(StockColumn.SYMBOL).isInCollection(randomStockList))

    logger.info("Preparing stock dataset by calculating averages, max and percentiles")
    //Get average closing amount for each symbol in pool
    val stockAverageCost = filteredByNameStockDf.rdd.map((s) => {
      (s.getString(1), s.getDouble(2))
    }).mapValues((v) => {
      (v, 1)
    }).reduceByKey{
      case ((sumL, countL), (sumR, countR)) =>
        (sumL + sumR, countL + countR)
    }.mapValues {
      case (sum , count) => sum / count
    }.collect()


    //Get max closing amount for each symbol in pool
    val stockMaxCost = filteredByNameStockDf.rdd.map((s) => {
      (s.getString(1), s.getDouble(2))
    }).reduceByKey((v1, v2) => {
      (Math.max(v1, v2))
    }).collect


    //Get all four percentile closing amounts for each symbol in the pool
    val stockPercentileCost = new Array[(String, Array[Double])](randomStockList.length);
    val stockAvgLoss = new Array[(String, Double)](randomStockList.length)

    0 to randomStockList.length-1 foreach { i => ({
      //Get percentiles
      val stocks = filteredByNameStockDf.filter(filteredByNameStockDf.col(StockColumn.SYMBOL)===randomStockList(i))
        .stat.approxQuantile(StockColumn.CLOSE, Array(0.25, 0.50, 0.75, 0.95), 0.0)
      stockPercentileCost(i) = (randomStockList(i).toString(), stocks)

      //Get a random subset of each stock
      val randomNeighborStocks = filteredByNameStockDf.filter(filteredByNameStockDf.col(StockColumn.SYMBOL)===randomStockList(i)).rdd
        .map((s) => {
        (new Date(s.getTimestamp(0).getTime), s.getDouble(2))
      }).takeSample(false, randomLossStockCount, System.currentTimeMillis())

      val targetStock = filteredByNameStockDf.filter(filteredByNameStockDf.col(StockColumn.SYMBOL)===randomStockList(i)).rdd
        .map((s) => {
        (new Date(s.getTimestamp(0).getTime), s.getDouble(2))
      }).takeSample(false, 1, System.currentTimeMillis())(0)

      //calculate the average loss for the stock, we will use this value later in determining the best policy
      stockAvgLoss(i) = (randomStockList(i), Utils.calculateAvgStockLoss(randomNeighborStocks, targetStock))

    })}

    val normalizedStockAvgLoss = Utils.normalizeLosses(stockAvgLoss)

    //Create the final stock list. Now that we have the stats on all selected stocks for all available dates, we can filter by simulation date
    val stockRdd = filteredByNameStockDf.filter(stockDf.col(StockColumn.TIMESTAMP).geq(new Timestamp(startDate.getTime)) &&
      stockDf.col(StockColumn.TIMESTAMP).leq(new Timestamp(endDate.getTime)))
      .rdd.map((s) => {
      val timestamp = new Date(s.getTimestamp(0).getTime)
      val name = s.getString(1)
      val max = Utils.getItemByKey(stockMaxCost, name)
      val avg = Utils.getItemByKey(stockAverageCost, name)
      val cost = s.getDouble(2)
      val percentiles = Utils.getItemsByKey(stockPercentileCost, name)
      val avgLoss = Utils.getItemByKey(normalizedStockAvgLoss, name)

      (new Stock(timestamp, name, cost, max, avg, percentiles(0), percentiles(1), percentiles(2), percentiles(3), avgLoss, 0))
    })

    //get a list of days in between start and end dates of simulation
    val datesInBetween = Utils.getDatesInBetween(startDate, endDate);

    //Make sure simulation date list is not empty after removing holiday and weekend dates.
    if (datesInBetween.length == 0) {
      logger.error("After removing weekends and holidays that the stock market observes, there are 0 dates remaining to run simulation on")
      System.exit(0)
    }


    1 to numberOfSimulations foreach{ i => ({
      logger.info("Starting simulation " + i.toString)
      startSimulation(startingFund, i, datesInBetween, stockRdd, outputDir )
      logger.info("Completed simulation " + i.toString)

    })}
  }

  def startSimulation(startingFund: Double, simulationID: Int, days:ListBuffer[Date], stocks: RDD[Stock], outputFileDir: String ): Unit = {
    var recentProfile:RDD[Stock] = null
    val logger: Logger = LoggerFactory.getLogger(getClass)

    days.foreach((d) => {
      val todayStocks = stocks.filter((s) => {
        s.TimeStamp.equals(new Timestamp(d.getTime))
      })

      logger.info("buying stocks for day: " + d.toString )

      if (recentProfile == null) {
        //this is the first time we are buying stocks.
        recentProfile = BuyStockPolicy.firstTimePurchase(startingFund, todayStocks)

        recentProfile.map((s) => {
          (new SimpleDateFormat("yyyy-MM-dd").format(s.TimeStamp), s.Share * s.Closing)
        }).reduceByKey(_ + _).saveAsTextFile(outputFileDir + "/" +simulationID + "/results/" + new SimpleDateFormat("yyyy-MM-dd").format(d))

        recentProfile.map((s) => {
          (s.Symbol, s.Closing, s.Share)
        }).saveAsTextFile(outputFileDir + "/" +simulationID + "/"+ new SimpleDateFormat("yyyy-MM-dd").format(d))
      } else {
        val bundle = SellStockPolicy.percentileBased(todayStocks, recentProfile)
        //These is the money made after selling stocks
        val newFund = bundle._2
        //These are the stocks that weren't sold
        val notSoldProfile = bundle._1

        if (newFund > 0)  {
          logger.info("sold stocks and made $" + newFund )
          //use remaining funds to buy more stocks
          val newProfile = BuyStockPolicy.lossBased(newFund, todayStocks)
          //merge stocks that user didn't sell as well as the new stocks he purchased to create a new profile
          recentProfile = newProfile.union(notSoldProfile).groupBy((s) => {
            s.Symbol
          }).map((st) => {
            var list = new ListBuffer[Stock]()
            st._2.foreach((s) => {
              list += (new Stock(s.TimeStamp, s.Symbol, s.Closing, s.Max, s.Avg, s.Percentile25, s.Percentile50, s.Percentile75, s.Percentile95, s.AvgLoss, s.Share))
            })
            val s = list(0)
            val share = list.map((l) => {
              l.Share
            }).reduce((a, b) => {
              a + b
            })
            (new Stock(s.TimeStamp, s.Symbol, s.Closing, s.Max, s.Avg, s.Percentile25, s.Percentile50, s.Percentile75, s.Percentile95, s.AvgLoss, share))

          })

         recentProfile.map((s) => {
            (new SimpleDateFormat("yyyy-MM-dd").format(s.TimeStamp), s.Share * s.Closing)
          }).reduceByKey(_ + _).saveAsTextFile(outputFileDir + "/" +simulationID + "/results/" + new SimpleDateFormat("yyyy-MM-dd").format(d))

          recentProfile.map((s) => {
            (s.Symbol, s.Closing, s.Share)
          }).saveAsTextFile(outputFileDir + "/" +simulationID + "/"+ new SimpleDateFormat("yyyy-MM-dd").format(d))

        } else {
          recentProfile = notSoldProfile
          notSoldProfile.map((s) => {
            (s.Symbol, s.Closing, s.Share)
          }).saveAsTextFile(outputFileDir + "/" +simulationID + "/"+ new SimpleDateFormat("yyyy-MM-dd").format(d))

          notSoldProfile.map((s) => {
            (new SimpleDateFormat("yyyy-MM-dd").format(s.TimeStamp), s.Share * s.Closing)
          }).reduceByKey(_ + _).saveAsTextFile(outputFileDir + "/" +simulationID + "/results/" + new SimpleDateFormat("yyyy-MM-dd").format(d))
        }

      }
    })
  }

}
