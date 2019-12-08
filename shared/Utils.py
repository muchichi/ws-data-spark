from pyspark.sql import SparkSession, DataFrame
from pyspark import SparkContext, SparkConf
from geopy.distance import geodesic
import pandas as pd


class Common:
	
	@staticmethod
	def spark_context() -> SparkSession:
		conf = SparkConf().setAppName('eq_loc').setMaster('local')
		sc = SparkContext(conf=conf).getOrCreate()
		sq = SparkSession(sc)
		return sq
	
	@staticmethod
	def get_df(path: str) -> DataFrame:
		df = Common.spark_context().read.csv(path, header=True, inferSchema=True)
		return df
	
	@staticmethod
	def get_poi(lat, long):
		dis_val = {}
		initail = (lat, long)
		poi = Common.get_df(path='data/POIList.csv').toPandas()
		for i, row in poi.iterrows():
			dest = (row['Latitude'], row['Longitude'])
			distance = geodesic(initail, dest).miles
			dis_val['poi'] = row['POIID']
			dis_val['distance'] = distance
		df = pd.DataFrame(dis_val).sort_values('distance')['poi'].values[0]
