from pyspark.sql import SparkSession, DataFrame, Column
from pyspark import SparkContext, SparkConf
from geopy.distance import geodesic
import pandas as pd
from pyspark.sql.functions import udf, array, struct, split
from pyspark.sql.types import StringType, IntegerType, FloatType, DoubleType


class Common:
	poi = None
	
	@staticmethod
	def spark_context() -> SparkSession:
		conf = SparkConf().setAppName('eq_loc').setMaster('local')
		sc = SparkContext.getOrCreate(conf=conf)
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
		print(f'processing ({lat},{lng})')
		lst = []
		initial = (lat, lng)
		print(f'>>>> {initial} >>>>')
		for i, row in Common.poi.iterrows():
			# print(i)
			dis_val = {}
			dest = (row[' Latitude'], row['Longitude'])
			poiid = row['POIID']
			distance = geodesic(initial, dest).miles
			# print(f' {dest} | {poiid} | {distance}')
			dis_val['poi'] = poiid
			dis_val['distance'] = distance
			lst.append(dis_val)
			# print(lst)
		# print(f'------- {lst} ------')
		df = pd.DataFrame(lst)
		if df is not None:
			print(df.head())
			req_poi = df.sort_values('distance')['poi'].values[0]
			dis_mile = df.sort_values('distance')['distance'].values[0]
			print(f'======> ({req_poi},{dis_mile})')
			result = req_poi + ',' + str(dis_mile)
			return result
		else:
			return ''
	
	@staticmethod
	def assign_poi() -> [DataFrame, DataFrame]:
		df = Common.get_df('data/DataSample.csv').drop_duplicates([' TimeSt', 'Latitude', 'Longitude'])
		distance = udf(Common.get_poi, StringType())
		df = df.withColumn('result', distance(struct('Latitude', 'Longitude')))
		spl_c = split(df['result'], ',')
		df = df.withColumn('Distance', spl_c.getItem(1).astype(DoubleType()))
		df = df.withColumn('POI', spl_c.getItem(0))
		dfs = df
		df_poi_average_distance = dfs.groupBy('POI').avg('Distance')
		return [df, df_poi_average_distance]
