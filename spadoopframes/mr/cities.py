from mrjob.job import MRJob

def is_int_or_empty(x):
    x = x.strip()
    return x == '' or x.lstrip('-').isdigit()

class CityZipCount(MRJob):

    def mapper(self, _, line):
        # fields: name, state, county, population, zips, id
        fields = line.rstrip('\n').split('\t')
        # bad lines: wrong number of fields or non-numeric population/id
        if len(fields) != 6 or not is_int_or_empty(fields[3]) or not is_int_or_empty(fields[5]):
            return
        zips = fields[4].strip()
        if zips == '':
            zip_count = 0
        else:
            zip_count = len(zips.split())
        yield zip_count, 1

    def combiner(self, key, values):
        # partial sums per mapper; same operation as the reducer, so it is safe
        yield key, sum(values)

    def reducer(self, key, values):
        yield key, sum(values)

if __name__ == '__main__':
    CityZipCount.run()
