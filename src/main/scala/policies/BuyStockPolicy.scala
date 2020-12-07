package org.amnairfan.hw3

import org.apache.spark.rdd.RDD

object BuyStockPolicy {
  //This is our policy for buying stocks
  def lossBased(fund: Double, todayStocks: RDD[Stock]): RDD[Stock] = {
    //We choose n stocks randomly from the pool but base our investment on the likelihood of losses.
    //For example, if a stock has a high avg loss, we will invest less money
    //The lower the loss the more funds we will use to invest.
    val rnd = new scala.util.Random
    val start = 3
    val end = 6
    val sampleCount = (start + rnd.nextInt( (end - start) + 1 ))
    val randomStocks = todayStocks.takeSample(false, sampleCount, System.currentTimeMillis()).map((s) => {
      s.Symbol
    })
    val stockPool = todayStocks.filter((s) => {
      randomStocks.contains(s.Symbol)
    })
    val totalLoss = stockPool.map((s) => {
      s.AvgLoss
    }).reduce((a, b) => {
      a + b
    })
    val maxTotal = stockPool.map((s) => {
      (totalLoss - s.AvgLoss)
    }).reduce((a, b) => {
      a + b
    })
    (stockPool.map((s) => {
      val perc = (totalLoss - s.AvgLoss)/maxTotal
      val share = (perc * fund)/s.Closing
      (new Stock(s.TimeStamp, s.Symbol, s.Closing, s.Max, s.Avg, s.Percentile25, s.Percentile50, s.Percentile75, s.Percentile95, s.AvgLoss, share))
    }))

  }


  //This is our policy for buying stocks
  def firstTimePurchase(fund: Double, todayStocks: RDD[Stock]): RDD[Stock] = {
    //We choose n stocks randomly from the pool but base our investment on the likelihood of losses.
    //For example, if a stock has a high avg loss, we will invest less money
    //The lower the loss the more funds we will use to invest.

    val stockPool = todayStocks.filter((s) => {
      true
    })
    val totalLoss = stockPool.map((s) => {
      s.AvgLoss
    }).reduce((a, b) => {
      a + b
    })
    val maxTotal = stockPool.map((s) => {
      (totalLoss - s.AvgLoss)
    }).reduce((a, b) => {
      a + b
    })
    (stockPool.map((s) => {
      val perc = (totalLoss - s.AvgLoss)/maxTotal
      val share = (perc * fund)/s.Closing
      (new Stock(s.TimeStamp, s.Symbol, s.Closing, s.Max, s.Avg, s.Percentile25, s.Percentile50, s.Percentile75, s.Percentile95, s.AvgLoss, share))
    }))

  }

}
