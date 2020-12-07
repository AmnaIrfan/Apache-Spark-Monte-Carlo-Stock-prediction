package org.amnairfan.hw3

import org.apache.spark.rdd.RDD

object SellStockPolicy {
  //This will return the total amount investor made after selling and the profile of stocks that he didn't sell
  def percentileBased(todayStocks: RDD[Stock], previousProfile: RDD[Stock]): (RDD[Stock], Double) ={

    val currentStockNames = previousProfile.map((s) => {
      s.Symbol
    }).collect()

    val currentStockInvestment = previousProfile.map((s) => {
      (s.Symbol, s.Share)
    }).collect()

    //Calculate the total amount the investor will make if they sell their stock today.
    val sellShare = todayStocks.filter((s) => {
      s.Closing > s.Percentile75 && currentStockNames.contains(s.Symbol)
    }).map((s) => {
      val share = Utils.getItemByKey(currentStockInvestment, s.Symbol)
      share * s.Closing
    })

    var fund = 0.0
    if (sellShare.count() > 0) {
      fund = sellShare.reduce((a, b ) => {
        a + b
      })
    }


    //Create a new profile on stocks that have not been sold but their value changed
    val newProfile = todayStocks.filter((s) => {
      s.Closing <= s.Percentile75 && currentStockNames.contains(s.Symbol)
    }).map((s) => {
      val share = Utils.getItemByKey(currentStockInvestment, s.Symbol)
      (new Stock(s.TimeStamp, s.Symbol, s.Closing, s.Max, s.Avg, s.Percentile25, s.Percentile50, s.Percentile75, s.Percentile95, s.AvgLoss, share ))
    })

    (newProfile, fund)

  }
}
