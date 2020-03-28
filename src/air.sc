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

  class Service(val frame: DataFrame) {

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
      joined.groupBy(j => j._3.country).map {
        case (k, v) => (k, v.flatten {
          case (airline, _, _) => Seq(airline).filter(_.name.equals(airline))
        })
      }.map {
        case (k, v) => (k, v.length)
      }
    }

    def getVisitedAirports(joined: Seq[(Airline, Route, Airport)], airline: String):Map[String, Seq[Airport]] = {
      joined.groupBy(j => j._3.country).map {
        case (k, v) => (k, v.filter(_._1.name.equals(airline)).distinct.flatten {
          case (_, _, airport) => Seq(airport)
        })
      }
    }

    def countryStat(airline: String):Map[String, (Int, Seq[Airport])] = {
      val joined = mergeFull(mergeAirlinesRoutes(frame.airlines, frame.routes), frame.airports)
      val visitsCount = countVisits(joined, airline)
      val visitedAirports = getVisitedAirports(joined, airline)

      visitsCount.map{
        case (k, v) => (k, (v, visitedAirports(k)))
      }
    }

  }
}
