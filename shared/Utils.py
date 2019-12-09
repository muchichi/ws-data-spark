from pyspark.sql import SparkSession, DataFrame
from pyspark import SparkContext, SparkConf
from geopy.distance import geodesic
import pandas as pd
from pyspark.sql.functions import udf, array, struct
from pyspark.sql.types import StringType, IntegerType


class Common:
	
	poi = None
	
	@staticmethod
	def spark_context() -> SparkSession:
		conf = SparkConf().setAppName('eq_loc').setMaster('local')
		sc = SparkContext.getOrCreate(conf=conf)  # SparkContext(conf=conf).getOrCreate()
		sq = SparkSession(sc)
		return sq
	
	@staticmethod
	def get_df(path: str) -> DataFrame:
		df = Common.spark_context().read.csv(path, header=True, inferSchema=True)
		return df
	
	@staticmethod
	def get_poi(cor):
		if Common.poi is None:
			Common.poi = Common.get_df(path='data/POIList.csv').toPandas()
			print(Common.poi.head())
		lat = cor['Latitude']
		lng = cor['Longitude']
		print(f'mussie processing {lat},{lng}')
		lst = []
		initial = (lat, lng)
		print(f'>>>> {initial} >>>>')
		#
		for i, row in Common.poi.iterrows():
			print(i)
			dis_val = {}
			dest = (row[' Latitude'], row['Longitude'])
			poiid = row['POIID']
			distance = geodesic(initial, dest).miles
			print(f' {dest} | {poiid} | {distance}')
			dis_val['poi'] = poiid
			dis_val['distance'] = distance
			lst.append(dis_val)
			print(lst)
		print(f'////  {lst}////')
		# df = pd.DataFrame.from_dict(dis_val, orient='index', columns=['poi', 'distance'])
		df = pd.DataFrame(lst)
		if df is not None:
			dis_mile = df.sort_values('distance')['poi'].values[0]
			return dis_mile
		else:
			return ''
	
	# @staticmethod
	# @udf(StringType())
	# def get_poi_ii(lst):
	# 	print(f'mussie processing({lst[0]},{lst[1]})')
	# 	dis_val = {}
	# 	initail = (lst[5], lst[6])
	# 	poi = Common.get_df(path='data/POIList.csv').toPandas()
	# 	for i, row in poi.iterrows():
	# 		dest = (row[1], row[2])
	# 		distance = geodesic(initail, dest).miles
	# 		dis_val['poi'] = row[0]
	# 		dis_val['distance'] = distance
	# 	return pd.DataFrame(dis_val).sort_values('distance')['poi'].values[0]
		
	@staticmethod
	def assign_poi(df: DataFrame):
		distance = udf(Common.get_poi, StringType())
		df = df.withColumn('poi', distance(struct('Latitude', 'Longitude')))
		return df
	
	@staticmethod
	def sumk(x):
		return x['A'] + x['B']
	
	@staticmethod
	def test():
		sum_cols = udf(Common.sumk, IntegerType())
		spark = Common.spark_context()
		a = spark.createDataFrame([(101, 1, 16)], ['ID', 'A', 'B'])
		a.show()
		m = struct('A', 'B')
		print(m)
		a = a.withColumn('Result', sum_cols(struct('A', 'B')))
		a.show()

