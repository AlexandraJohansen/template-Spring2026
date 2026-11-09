from mrjob.job import MRJob
 
class CityZipCount(MRJob):
 
    def mapper(self, _, line):
        fields = line.strip().split('\t')
       
        if len(fields) < 5 or fields[0].strip() == '':
            return
        zip_field = fields[4].strip()
        
        if zip_field == '':
            zip_count = 0
        else:
            zip_count = len(zip_field.split())
        
        yield zip_count, 1
 
    def combiner(self, key, values):
        
        yield key, sum(values)
 
    def reducer(self, key, values):
       
        yield key, sum(values)
 
if __name__ == '__main__':
    CityZipCount.run()
 
