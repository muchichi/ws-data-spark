from shared.sp_utils import Common
from shared.ks_utils import KCommon
import warnings


if __name__ == '__main__':
	warnings.filterwarnings("ignore")
	
	## Spark implementaion
	# locations = Common.get_df('data/DataSample.csv').drop_duplicates([' TimeSt', 'Latitude', 'Longitude'])
	#
	# point_of_interest, avg_mean = Common.assign_poi(locations)
	# point_of_interest.show(10)
	# avg_mean.head()
	
	## Koalas implementation
	kcom = KCommon()
	kcom.std_avg_distance()
	
	
	
	
	
	

