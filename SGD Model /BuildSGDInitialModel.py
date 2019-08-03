import psycopg2
from sshtunnel import SSHTunnelForwarder
import pandas as pd
from datetime import datetime
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error
from sklearn.externals import joblib
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import SGDRegressor



def outer_wrapper(line):
	def leavetimes_trips(line):
		flag=True
		while flag==True:
		    try: 
		        mystring = str("SELECT * FROM rt_leavetimes_db_2018 WHERE trip_id IN (SELECT distinct trip_id FROM rt_trips_db_2018 WHERE line_id=\'"+line+"\')")   
		        commands = (mystring)   
		                   

		        with SSHTunnelForwarder(
		             (private.address, 22),

		             ssh_username=private.username,
		             ssh_password=private.password, 
		             remote_bind_address=(private.address, 5432)) as server:

		             server.start()
		             print ("server connected")

		             params = {
		                 'database': private.database,
		                 'user': private.user,
		                 'password': private.password,
		                 'host': private.host,
		                 'port': server.local_bind_port
		                 }
		             conn = psycopg2.connect(**params)
		             curs = conn.cursor()
		             print ("database connected")

		             df_leavetimes_trips = pd.read_sql_query(commands, conn)


		             curs.close()
		             conn.commit()
		             conn.close()
		             return df_leavetimes_trips
		             flag=False

		    except:
		        print ("Connection Failed")

	df_leavetimes_trips=leavetimes_trips(line)

	df_leavetimes_trips['round_secs']=round(df_leavetimes_trips['actual_arrival_time']/3600)*3600

	df_leavetimes_trips['day_of_service'] =  pd.to_datetime(df_leavetimes_trips['day_of_service'])

	df_leavetimes_trips['day_of_service'] = df_leavetimes_trips['day_of_service'].dt.strftime('%Y-%m-%d')

	df_weather=pd.read_csv('weather_2018_clean.csv',  keep_default_na=True, sep=',\s+', delimiter=',', skipinitialspace=True)

	merged_df = df_leavetimes_trips.merge(df_weather, how = 'inner', on = ['day_of_service', 'round_secs'])

	merged_df['Delay']=(merged_df['actual_arrival_time']-merged_df['planned_arrival_time'])/60

	def convert_time_period(seconds):
	    if seconds >= 0 and seconds < 25200:
	        return 0
	    elif seconds >= 25200 and seconds < 36000:
	        return 5
	    elif seconds >= 36000 and seconds < 54000:
	        return 3
	    elif seconds >= 54000 and seconds < 61200:
	        return 4
	    elif seconds >= 61200 and seconds < 68400:
	        return 6
	    elif seconds >= 68400 and seconds < 79200:
	        return 2
	    elif seconds >= 79200:
	        return 1

	merged_df['Time_period'] = merged_df['planned_arrival_time'].apply(convert_time_period)

	def weekday(date):
	    mydatetime = datetime.strptime(date, "%Y-%m-%d")
	    weekday=mydatetime.weekday()
	    if weekday <=4:
	        return 1
	    elif weekday<=6: 
	        return 0
	    
	merged_df['weekday'] = merged_df['day_of_service'].apply(weekday)

	df_trim=merged_df.drop(['day_of_service','datasource', 'trip_id', 'stop_point_id', 'planned_arrival_time', 'planned_dept_time', 'actual_arrival_time', 'actual_dept_time', 'vehicle_id', 'passengers', 'passengers_in', 'passengers_out', 'distance', 'suppressed', 'justification_id', 'last_update', 'note', 'round_secs', 'wetb', 'dewpt', 'vappr', 'msl'], axis=1)

	df_trim=df_trim.dropna()

	df_trim = df_trim.sample(frac=1).reset_index(drop=True)

	X = df_trim.drop('Delay', axis=1)
	y= df_trim['Delay']
 
 #######################################

	line_list= ['75',
	 '68X',
	 '13',
	 '41A',
	 '46E',
	 '104',
	 '7A',
	 '18',
	 '32',
	 '25A',
	 '38A',
	 '76',
	 '33B',
	 '14C',
	 '37',
	 '33E',
	 '9',
	 '4',
	 '70D',
	 '15B',
	 '56A',
	 '65B',
	 '140',
	 '67X',
	 '68A',
	 '66',
	 '61',
	 '33X',
	 '31',
	 '11',
	 '114',
	 '43',
	 '41D',
	 '130',
	 '51X',
	 '49',
	 '69',
	 '41X',
	 '7',
	 '15',
	 '122',
	 '40',
	 '31D',
	 '27A',
	 '40D',
	 '111',
	 '25D',
	 '54A',
	 '116',
	 '145',
	 '7D',
	 '76A',
	 '17',
	 '15A',
	 '38B',
	 '185',
	 '120',
	 '45A',
	 '83A',
	 '25B',
	 '38D',
	 '84',
	 '63',
	 '17A',
	 '16D',
	 '70',
	 '15D',
	 '32X',
	 '41B',
	 '39',
	 '84X',
	 '25',
	 '14',
	 '31B',
	 '77A',
	 '79',
	 '66X',
	 '33A',
	 '31A',
	 '38',
	 '84A',
	 '238',
	 '68',
	 '236',
	 '16C',
	 '220',
	 '161',
	 '27X',
	 '46A',
	 '33',
	 '102',
	 '41C',
	 '53',
	 '27',
	 '151',
	 '66B',
	 '42',
	 '67',
	 '142',
	 '40E',
	 '150',
	 '47',
	 '270',
	 '44B',
	 '65',
	 '239',
	 '40B',
	 '44',
	 '59',
	 '7B',
	 '79A',
	 '77X',
	 '33D',
	 '184',
	 '39X',
	 '1',
	 '51D',
	 '42D',
	 '29A',
	 '83',
	 '69X',
	 '39A',
	 '41',
	 '27B',
	 '66A',
	 '16',
	 '25X',
	 '26',
	 '118',
	 '123']

	binary="{0:08b}".format(line_list.index(line))
	numbers=list(binary)
	numbers = [ int(x) for x in numbers ]
	print(numbers)

	X['Col_1']=numbers[0]
	X['Col_2']=numbers[1]
	X['Col_3']=numbers[2]
	X['Col_4']=numbers[3]
	X['Col_5']=numbers[4]
	X['Col_6']=numbers[5]
	X['Col_7']=numbers[6]
	X['Col_8']=numbers[7]



	XTrain, XTest, yTrain, yTest = train_test_split(X, y, test_size = 0.2, random_state = 0)


	model = SGDRegressor(loss='huber',
                      penalty='l2', 
                      alpha=0.0001, 
                      fit_intercept=False, 
                      n_iter=5, 
                      shuffle=True, 
                      verbose=1, 
                      epsilon=0.1, 
                      random_state=42, 
                      learning_rate='invscaling', 
                      eta0=0.01, 
                      power_t=0.5)

	model.partial_fit(XTrain, yTrain)

	SGDpredictions = model.predict(XTest)
	rmse=mean_squared_error(yTest, SGDpredictions) ** 0.5




	with open("SGD_models_report.txt", "a") as myfile:
	 message= "\nLine: "+str(line)+ " RMSE: "+str(rmse)
	 myfile.write(message)

	filename = 'SGD_joblib_files/SGD_original_model.sav'
	joblib.dump(model, filename)

	

outer_wrapper('75')



