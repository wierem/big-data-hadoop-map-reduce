from mrjob.job import MRJob
from mrjob.step import MRStep


class MRMap(MRJob):

    def mapper(self, _, line):
        (vendor_id, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance,
         pickup_long, pickup_lat, rate_code_id, store_and_fwd_flag, dropoff_long, dropof_lat, payment_type, fare_amount,
         extra, mta_tax,  tip_amount, tolls_amount, improvement_surcharge, total_amount) = line.split(',')
        yield passenger_count, 1

    def reducer(self, passengers, values):
        yield passengers, sum(values)




if __name__ == "__main__":
    MRMap.run()

