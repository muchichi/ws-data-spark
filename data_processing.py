from shared.Utils import Common
import pandas as pd

if __name__ == '__main__':
	
	locations = Common.get_df('data/DataSample.csv').drop_duplicates([' TimeSt', 'Latitude', 'Longitude'])

	new_locations = Common.assign_poi(locations)
	new_locations.show(250)
	# Common.test()
	
	# ls = [{'poi': 'POI4', 'distance': 834.3764352890981}, {'poi': 'POI2', 'distance': 812.3764352890981}, {'poi': 'POI1', 'distance': 857.3764352890981}, {'poi': 'POI3', 'distance': 810.3764352890981}]
	# df = pd.DataFrame(ls).sort_values('distance')
	# print(df)
	# print(df['poi'].values[0])
	

