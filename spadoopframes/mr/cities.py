from mrjob.job import MRJob

class CityZipCount(MRJob):

    def mapper(self, _, line):
        # fields: name, state, county, population, zips, id
        fields = line.rstrip('\n').split('\t')
        # bad lines: wrong number of fields or non-numeric population/id
        # (an empty population or id is allowed)
        if len(fields) != 6:
            return
        pop = fields[3].strip()
        cid = fields[5].strip()
        if pop != '' and not pop.lstrip('-').isdigit():
            return
        if cid != '' and not cid.lstrip('-').isdigit():
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
 
