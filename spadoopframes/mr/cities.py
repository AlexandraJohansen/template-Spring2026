from mrjob.job import MRJob

class CityZipCount(MRJob):

    def mapper(self, _, line):
        # fields: name, state, county, population, zips, id
        fields = line.rstrip('\n').split('\t')
        n = len(fields)
        # well formed: 6 fields, numeric (or empty) population and id
        well_formed = False
        if n == 6:
            pop = fields[3].strip()
            cid = fields[5].strip()
            well_formed = ((pop == '' or pop.lstrip('-').isdigit()) and
                           (cid == '' or cid.lstrip('-').isdigit()))
        # a city with no zips is also fine as long as it has a name, state and county
        no_zips = (n >= 3 and (n < 5 or fields[4].strip() == '') and
                   fields[0].strip() != '' and fields[1].strip() != '' and fields[2].strip() != '')
        if not (well_formed or no_zips):
            return
        zips = fields[4].strip() if n >= 5 else ''
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
