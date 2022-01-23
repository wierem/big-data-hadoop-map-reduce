from mrjob.job import MRJob
from mrjob.step import MRStep


class MRMap(MRJob):

    def mapper(self, _, line):
        (vendor_id, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance,
         pickup_long, pickup_lat, rate_code_id, store_and_fwd_flag, dropoff_long, dropof_lat, payment_type, fare_amount,
         extra, mta_tax,  tip_amount, tolls_amount, improvement_surcharge, total_amount) = line.split(',')
        if passenger_count == '':
            passenger_count = 0
        yield vendor_id, float(passenger_count)

    def reducer(self, vendor, values):
        total_passengers = 0
        total_trips = 0
        for value in values:
            total_passengers += value
            total_trips += 1
        yield vendor, (total_passengers, total_trips, total_passengers/total_trips)




if __name__ == "__main__":
    MRMap.run()

