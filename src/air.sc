//package ru.philit.bigdata.vsu.other

import scala.io.Source
import scala.util.Try

object airlines {

  case class Route(
                    airlineCode: String,
                    airlineId: Int,
                    sourceAirportCode: String,
                    sourceAirportId: Int,
                    targetAirportCode: String,
                    targetAirportId: Int
                  )


  case class Airline(
                      id: Int,
                      name: String,
                      iata: String,
                      icao: String,
                      country: String
                    )

  case class Airport(
                      id: Int,
                      name: String,
                      city: String,
                      country: String,
                      iata: String,
                      icao: String
                    )

  case class DataFrame(
                        routes: Seq[Route],
                        airlines: Seq[Airline],
                        airports: Seq[Airport],
                        countries: Map[String, String]
                      )
  /*Задание: используя датасет из проекта написать класс-сервис с методом countryStat(airline: String).
  Входной параметр - название авиакомпании или сокращение типа DME.
  Метод должен вернуть Map с ключом из названия страны и значением - количество посещений, и посещенные аэропорты в виде списка.
  Считать нужно все страны кроме домашней страны и аэропортов домашней страны*/
  class Service(val frame: DataFrame) {
    case class Output()

    def mergeAirlinesRoutes(airlines:Seq[Airline], routes: Seq[Route]): Seq[(Airline, Route)] = for {
      airline <- airlines
      route <- routes
      if airline.id == route.airlineId
    } yield (airline, route)

    def mergeFull(airlines_and_routes: Seq[(Airline, Route)], airports: Seq[Airport]): Seq[(Airline, Route, Airport)] = for {
      (airline, route) <- airlines_and_routes
      airport <- airports
      if !airline.country.toLowerCase.equals(airport.country.toLowerCase()) && (
        route.targetAirportCode.toLowerCase.equals(airline.iata.toLowerCase) ||
          route.targetAirportCode.toLowerCase.equals(airline.icao.toLowerCase))
    } yield (airline, route, airport)

    def countVisits(joined: Seq[(Airline, Route, Airport)], airline: String):Map[String, Int] = {
      //joined.flatMap (j => Seq(j._3)).map(airpot => Seq(airpot.country, airpot))
      joined.groupBy(j => j._3.country).map{
        case (k, v) => (k, v.flatten{
          case (air, _, _) => Seq(air).filter(_.name.equals(airline))
        })
      }.map {
        case(k, v) => (k, v.length)
      }
    }

    def visitedAirports(joined: Seq[(Airline, Route, Airport)], airline: String):Seq[Airport] = {
      joined.filter(j => j._1.name.equals(airline)).flatten{
        case (_, _, airport) => Seq(airport)
      }.distinct
    }

  }
}
