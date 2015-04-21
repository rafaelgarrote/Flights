package com.stratio.model.spark

import scala.language.implicitConversions
import com.stratio.model.Flight
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

class FlightCsvReader(self: RDD[String]) {

    /**
     *
     * Parser the csv file with the format described in te readme.md file to a Fligth class
     *
     */
    def toFlight: RDD[Flight] = self.map(f => Flight.apply(f.split(",")))

    /**
     *
     * Obtain the parser errors
     *
     */
    def toErrors: RDD[(String, String)] = self.flatMap(l => Flight.extractErrors(l.split(",")).map((_,l)))
  }

  class FlightFunctions(self: RDD[Flight]) {

    /**
     *
     * Obtain the minimum fuel's consumption using a external RDD with the fuel price by Year, Month
     *
     */
    def minFuelConsumptionByMonthAndAirport(fuelPrice: RDD[String]): RDD[(String, (Short, Short))] = {
      val fuel = fuelPrice.map(p => p.split(",")).map(p => ((p(0).toShort, p(1).toShort), p(2).toFloat))
      val flightFuel: RDD[((Short, Short), (Flight, Option[Float]))] = self.map(f => ((f.date.year().get.toShort, f.date.monthOfYear().get.toShort), f)).leftOuterJoin(fuel)
      flightFuel.map(f => ((f._2._1.origin,f._1),f._2._1.distance * f._2._2.get)).aggregateByKey(0f)(
          (comb: Float, fuel) => comb + fuel,
          (combAcc: Float, comb: Float) => combAcc + comb
        ).map(f => (f._1._1, (f._1._2, f._2))).combineByKey(
          (airport: ((Short, Short), Float)) => airport,
          (comb: ((Short, Short), Float), airport) => if(airport._2 < comb._2) airport else comb,
          (combAcc: ((Short, Short), Float), comb: ((Short, Short), Float)) => if(comb._2 < combAcc._2) comb else combAcc
        ).map(a => (a._1, a._2._1))
    }



    /**
     *
     * Obtain the average distance fliyed by airport, taking the origin field as the airport to group
     *
     */
    def averageDistanceByAirport: RDD[(String, Float)] = self.map(f => (f.origin, f.distance)).combineByKey(
        (dist: Int) => (dist, 1),
          (comb: (Int, Int), dist) => (comb._1 + dist, comb._2 + 1),
          (combAcc: (Int, Int), comb: (Int, Int)) => (combAcc._1 + comb._1, combAcc._2 + comb._2))
        .map((d:(String, (Int, Int))) => (d._1, (d._2._1.toFloat / d._2._2.toFloat)))


  /**
    * A Ghost Flight is each flight that has arrTime = -1 (that means that the flight didn't land where was supposed to
    * do it).
    *
    * The correct assign to those flights will be :
    *
    * The ghost's flight arrTime would take the data of the nearest flight in time that has its flightNumber and which
    * deptTime is lesser than the ghost's flight deptTime plus a configurable amount of seconds, from here we will call
    * this flight: the saneated Flight
    *
    * So if there is no sanitized Flight for a ghost Flight we will return the same ghost Flight but if there are some
    * sanitized Flight we had to assign the following values to the ghost flight:
    *
    * dest = sanitized Flight origin
    * arrTime = sanitized Flight depTime
    * csrArrTime = sanitized Flight csrDepTime
    * date =  sanitized Flight date
    *
    * --Example
    *   window time = 600 (10 minutes)
    *   flights before resolving ghostsFlights:
    *   flight1 = flightNumber=1, departureTime=1-1-2015 deparHour810 arrTime=-1 orig=A dest=D
    *   flight2 = flightNumber=1, departureTime=1-1-2015 departureTime=819 arrTime=1000 orig=C dest=D
    *   flight3 = flightNumber=1,  departureTime=1-1-2015 departureTime=815 arrTime=816 orig=B dest=C
    *   flight4 = flightNumber=2, departureTime=1-1-2015 deparHour810 arrTime=-1 orig=A dest=D
    *   flight5 = flightNumber=2, departureTime=1-1-2015 deparHour821 arrTime=855 orig=A dest=D
    *  flights after resolving ghostsFlights:
    *   flight1 = flightNumber=1, departureTime=1-1-2015 deparHour810 arrTime=815 orig=A dest=B
    *   flight2 = flightNumber=1, departureTime=1-1-2015 departureTime=819 arrTime=1000 orig=C dest=D
    *   flight3 = flightNumber=1,  departureTime=1-1-2015 departureTime=815 arrTime=816 orig=B dest=C
    *   flight4 = flightNumber=2, departureTime=1-1-2015 deparHour810 arrTime=-1 orig=A dest=D
    *   flight5 = flightNumber=2, departureTime=1-1-2015 deparHour821 arrTime=855 orig=A dest=D
    */

  def asignGhostFlights(elapsedSeconds: Int): RDD[Flight] = ???

  }


trait FlightDsl {

  implicit def flightParser(lines: RDD[String]): FlightCsvReader = new FlightCsvReader(lines)

  implicit def flightFunctions(flights: RDD[Flight]): FlightFunctions = new FlightFunctions(flights)
}

object FlightDsl extends FlightDsl

