from shared.sp_utils import Common
from shared.ks_utils import KCommon
import warnings

if __name__ == '__main__':
	warnings.filterwarnings("ignore")
	
	##  Spark implementaion
	point_of_interest = Common.assign_poi()[0]
	avg_mean = Common.assign_poi()[0]
	point_of_interest.show(10)
	avg_mean.head()

	##  Koalas implementation
	# kcom = KCommon()
	# kcom.std_avg_distance()
