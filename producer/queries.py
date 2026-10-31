CURRENTLY_RUNNING_TRAINS = """
    query {
        currentlyRunningTrains {
            trainNumber
            departureDate
            trainLocations(orderBy: {timestamp: DESCENDING}, take: 1) {
                speed
                timestamp
                location
            }
        }
    }
"""

STATIONS = """
    query {
        stations {
            countryCode
            location
            name
            passengerTraffic
            shortCode
            uicCode
        }
    }
"""
