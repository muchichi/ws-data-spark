import databricks.koalas as ks
import pandas as pd
from geopy.distance import geodesic


class KCommon:
	
	def __init__(self):
		self.poi = self.read_file('data/POIList.csv')
		self.data_sample = self.read_file('data/DataSample.csv')
		
	def read_file(self, path: str) -> ks.DataFrame:
		df = ks.read_csv(path)
		return df
	
	def set_poi(self, lat, lng):
		initial = (lat, lng)
		lst = []
		for _, row in self.poi.iterrows():
			dis_val = {}
			dest = (row[' Latitude'], row['Longitude'])
			poiid = row['POIID']
			distance = geodesic(initial, dest).miles
			dis_val['poi'] = poiid
			dis_val['distance'] = distance
			lst.append(dis_val)
		df = pd.DataFrame(lst)
		if df is not None:
			print(df.head())
			req_poi = df.sort_values('distance')['poi'].values[0]
			dis_mile = df.sort_values('distance')['distance'].values[0]
			print(f'======> ({req_poi},{dis_mile})')
			result = [req_poi, dis_mile]
			return result
		else:
			return '', ''
	
	def assign_poi(self):
		self.data_sample.assign(distance=lambda x: self.set_poi(x['Latitude'], x['Longitude']))  #)lambda x: self.set_poi(x['Latitude'], x['Longitude'])[1])
		self.data_sample.assign(poi=lambda x: self.set_poi('Latitude', 'Longitude')[0]) #x.Latitude, x.Longitude
		self.data_sample.head(10)
		return self.data_sample
	
	def std_avg_distance(self):
		self.assign_poi()
		df_mean = self.data_sample.groupby('POIID').mean()
		df_mean.show()
		df_std = self.data_sample.groupby('POIID').std()
		df_std.show()

