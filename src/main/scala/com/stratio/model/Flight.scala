package com.stratio.model

import com.stratio.utils.ParserUtils
import org.joda.time.DateTime

sealed case class Cancelled (id: String) {override def toString: String = id}

object OnTime extends Cancelled (id ="OnTime")
object Cancel extends Cancelled (id ="Cancel")
object Unknown extends Cancelled (id ="Unknown")

case class Delays (
    carrier: Cancelled,
    weather: Cancelled,
    nAS: Cancelled,
    security: Cancelled,
    lateAircraft: Cancelled)

case class Flight (date: DateTime, //Tip: Use ParserUtils.getDateTime
    departureTime: Int,
    crsDepatureTime: Int,
    arrTime: Int,
    cRSArrTime: Int,
    uniqueCarrier: String,
    flightNum: Int,
    actualElapsedTime: Int,
    cRSElapsedTime: Int,
    arrDelay: Int,
    depDelay: Int,
    origin: String,
    dest: String,
    distance: Int,
    cancelled: Cancelled,
    cancellationCode: Int,
    delay: Delays)
{
  def isGhost: Boolean = arrTime == -1

  def departureDate: DateTime =
    date.hourOfDay.setCopy(departureTime.toString.substring(0, departureTime.toString.size - 2)).minuteOfHour
      .setCopy(departureTime.toString.substring(departureTime.toString.size - 2)).secondOfMinute.setCopy(0)

  def arriveDate: DateTime =
    date.hourOfDay.setCopy(departureTime.toString.substring(0, departureTime.toString.size - 2)).minuteOfHour
      .setCopy(departureTime.toString.substring(departureTime.toString.size - 2)).secondOfMinute.setCopy(0)
      .plusMinutes(cRSElapsedTime)
}

object Flight{

  /*
  *
  * Create a new Flight Class from a CSV file
  *
  */
  def apply(fields: Array[String]): Flight = {
    val year = ParserUtils.getDateTime(fields(0).toInt, fields(1).toInt, fields(2).toInt)
    new Flight(year,
      fields(4).toInt,
      fields(5).toInt,
      fields(6).toInt,
      fields(7).toInt,
      fields(8),
      fields(9).toInt,
      fields(11).toInt,
      fields(12).toInt,
      fields(14).toInt,
      fields(15).toInt,
      fields(16),
      fields(17),
      fields(18).toInt,
      parseCancelled(fields(21)),
      fields(22).toInt,
      new Delays(
        parseCancelled(fields(24)),
        parseCancelled(fields(25)),
        parseCancelled(fields(26)),
        parseCancelled(fields(27)),
        parseCancelled(fields(28))
      )
    )
  }

  /*
   *
   * Extract the different types of errors in a string list
   *
   */
  def extractErrors(fields: Array[String]): Seq[String] = fields.map(f => parse(f, fields.indexOf(f))).flatten.toSeq

  def parse(l:String, position:Int): Option[String] = {
    position match {
      case 19 | 20 | 23 | 8 | 16 | 17 => None
      case _ => ParserUtils.parseIntError(l)
    }
  }

  def range(l:String, position:Int): Option[String] = {
    position match {
      case 21|24|25|26|27|28 => if (l != 0 || l != 1)  Some("RangeError") else None
      case _ => None
    }
  }

  /*
  *
  * Parse String to Cancelled Enum:
  *   if field == 1 -> Cancel
  *   if field == 0 -> OnTime
  *   if field <> 0 && field<>1 -> Unknown
  */
  def parseCancelled(field: String): Cancelled = field match {
    case "1" => Cancel
    case "0" => OnTime
    case _ => Unknown
  }
}
