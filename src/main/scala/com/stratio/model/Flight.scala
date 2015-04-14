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

object Flight{

  /*
  *
  * Create a new Flight Class from a CSV file
  *
  */
  def apply(fields: Array[String]): Flight = {
    val year = ParserUtils.getDateTime(Integer.parseInt(fields.apply(0)), Integer.parseInt(fields.apply(1)), Integer.parseInt(fields.apply(2)))
    new Flight(year,
      Integer.parseInt(fields.apply(3)),
      Integer.parseInt(fields.apply(4)),
      Integer.parseInt(fields.apply(5)),
      Integer.parseInt(fields.apply(6)),
      fields.apply(7),
      Integer.parseInt(fields.apply(8)),
      Integer.parseInt(fields.apply(9)),
      Integer.parseInt(fields.apply(10)),
      Integer.parseInt(fields.apply(11)),
      Integer.parseInt(fields.apply(12)),
      fields.apply(13),
      fields.apply(14),
      Integer.parseInt(fields.apply(15)),
      parseCancelled(fields.apply(16)),
      Integer.parseInt(fields.apply(17)),
      new Delays(
        parseCancelled(fields.apply(18)),
        parseCancelled(fields.apply(19)),
        parseCancelled(fields.apply(20)),
        parseCancelled(fields.apply(21)),
        parseCancelled(fields.apply(22))
      )
    )
  }

  /*
   *
   * Extract the different types of errors in a string list
   *
   */
  def extractErrors(fields: Array[String]): Seq[String] = ???

  /*
  *
  * Parse String to Cancelled Enum:
  *   if field == 1 -> Cancel
  *   if field == 0 -> OnTime
  *   if field <> 0 && field<>1 -> Unknown
  */
  def parseCancelled(field: String): Cancelled = ???
}
