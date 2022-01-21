from mrjob.job import MRJob
from mrjob.step import MRStep

class MRWorldCount(MRJob):

    def mapper(self, _, line):
        words = line.split()
        for word in words:
            word.replace('.', '')
            yield word.lower(), 1

    def reducer(self, key, values):
        yield key, sum(values)

if __name__ == '__main__':
    MRWorldCount.run()