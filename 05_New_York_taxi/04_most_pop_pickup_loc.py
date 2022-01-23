from mrjob.job import MRJob
from mrjob.step import MRStep

class MRMap(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer),
            MRStep(mapper=self.mapper_get_keys,
                   reducer=self.reducer_get_sorted)
            ]

    def mapper(self, _, line):
        (vendor_id, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance,
         pickup_long, pickup_lat, rate_code_id, store_and_fwd_flag, dropoff_long, dropof_lat, payment_type, fare_amount,
         extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount) = line.split(',')

        pickup_lat = pickup_lat[:8]
        pickup_long = pickup_long[:9]
        yield (float(pickup_long), float(pickup_lat)), 1

    def reducer(self, localization, values):
        yield localization, sum(values)

    def mapper_get_keys(self, key, value):
        yield None, (value, key)

    def reducer_get_sorted(self, key, values):
        self.results = []
        for value in values:
            self.results.append((key, value))
        yield None, sorted(self.results, reverse=True)[:10]


if __name__ == "__main__":
    MRMap.run()

