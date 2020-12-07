package org.amnairfan.hw3

import java.util.Date

/*
* This class represents a single stock on a single day. We use this instance to buy, sell and represent stocks
* Some properties like share will be missing before simulation as at that point the stock for that day has not been bought
*
* @author Amna Irfan
*/

class Stock (val TimeStamp: Date,
             val Symbol: String,
             val Closing: Double,
             val Max: Double,
             val Avg: Double,
             val Percentile25: Double,
             val Percentile50: Double,
             val Percentile75: Double,
             val Percentile95: Double,
             val AvgLoss: Double,
             val Share: Double) extends Serializable {
}
