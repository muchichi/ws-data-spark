from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf


if __name__ == '__main__':
	conf = SparkConf().setAppName('eq_loc').setMaster('local')
	sc = SparkContext(conf=conf).getOrCreate()
	sq = SparkSession(sc)
	
	locations = sq.read.csv('data/DataSample.csv', header=True, inferSchema=True)
	locations.printSchema()
	print(locations.count())
	locations.show(5)
	
	locs = locations.drop_duplicates([' TimeSt', 'Latitude', 'Longitude'])
	print(locs.count())
	locs.show(5)

