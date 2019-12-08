from shared.Utils import Common


if __name__ == '__main__':
	
	locations = Common.get_df('data/DataSample.csv').drop_duplicates([' TimeSt', 'Latitude', 'Longitude'])
	locations.printSchema()
	print(locations.count())
	locations.show(5)
	
	
	

