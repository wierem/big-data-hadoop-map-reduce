from mrjob.job import MRJob
from mrjob.step import MRStep
import pandas as pd



class MRFlights(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer_init=self.reducer_init,
                   reducer=self.reducer)
        ]

    def configure_args(self):
        super(MRFlights, self).configure_args()
        self.add_file_arg('--airlines', help='Path to the airlines.csv')


    def mapper(self, _, line):
        (YEAR, MONTH, DAY, DAY_OF_WEEK, AIRLINE, FLIGHT_NUMBER, TAIL_NUMBER, ORIGIN_AIRPORT, DESTINATION_AIRPORT,
         SCHEDULED_DEPARTURE, DEPARTURE_TIME, DEPARTURE_DELAY, TAXI_OUT, WHEELS_OFF, SCHEDULED_TIME, ELAPSED_TIME,
         AIR_TIME, DISTANCE, WHEELS_ON, TAXI_IN, SCHEDULED_ARRIVAL, ARRIVAL_TIME, ARRIVAL_DELAY, DIVERTED, CANCELLED,
         CANCELLATION_REASON, AIR_SYSTEM_DELAY, SECURITY_DELAY, AIRLINE_DELAY,
         LATE_AIRCRAFT_DELAY, WEATHER_DELAY) = line.split(',')
        if CANCELLED == '':
            CANCELLED = 0
        CANCELLED = int(CANCELLED)
        yield AIRLINE, CANCELLED


    def reducer_init(self):
        self.airlineNames = {}
        with open('airlines.csv', 'r') as file:
            for line in file:
                code, full_name = line.split(',')
                full_name = full_name[:-1]
                self.airlineNames[code] = full_name


    def reducer(self, airline, values):
        total_cancelled = 0
        total_flights = 0
        for value in values:
            total_cancelled += value
            total_flights += 1
        yield self.airlineNames[airline], (total_cancelled, total_flights, total_cancelled/total_flights)

if __name__ == "__main__":
    MRFlights.run()