from pymongo import MongoClient
from datetime import datetime, timedelta
from threading import Thread
import requests
import json

def runScrap(logdoc,colln_qlog):
	print(logdoc)
	
	try:
		data = {
			"source_details":{
		    	"url":logdoc["source_details"]["url"]
		},
			"source_id":str(logdoc["source_id"]),
			"team_id":str(logdoc["team_id"]),
			"user_id":str(logdoc["user_id"]),
			"start_date":str(logdoc["next_run_req_param_date_range"]["start_date"]),
			"end_date":str(logdoc["next_run_req_param_date_range"]["end_date"])
		}

		response = requests.post(url='http://apiaquapi.101logix.com/api/manage_source/scrapAppSource', json=data)
		#response = requests.post(url='http://api.reviewanalytics.io/api/manage_source/scrapAppSource', json=data)
		response_json = response.json()
		is_success = response_json.get('success')
	except KeyError as kerr:
		print('KeyError: Key Not Found :', kerr)
		is_success = False
	except TypeError:
		print("TypeError: Check list of indices")
		is_success = False	

	if is_success == True:
		no_of_scrap_data = response_json.get('data').get('no_of_scrap_data')
		total_execution_time = response_json.get('data').get('total_execution_time')
		
		doquery = { "_id": logdoc["_id"] }
		newvalues = { "$set": { "executed":True,"error_occured":False,"no_of_scrap_data": no_of_scrap_data, "total_execution_time": total_execution_time, "excuted_at": datetime.now() } }
		print(newvalues)
		colln_qlog.update_one(doquery, newvalues)
	else:
		doquery = { "_id": logdoc["_id"] }
		newvalues = { "$set": { "executed":True,"error_occured":True,"excuted_at": datetime.now() } }
		print(newvalues)
		colln_qlog.update_one(doquery, newvalues)


#client = MongoClient('localhost',27017)
client = MongoClient('mongodb+srv://admin:aquapi22@aquapi.geny1.mongodb.net/review_analytics')
db = client.review_analytics
collection_queue_log = db.source_queue_managers

# try:
#     info = client.server_info()
#     print(info)
# except ServerSelectionTimeoutError:
#     print("mongo server is down.")

# array = list(collection_queue_log.find())
# print(array)
# exit()  

#current_datetime = datetime.datetime.now() # getting local time
current_datetime = datetime.utcnow() # getting UTC time
#current_datetime = current_datetime + timedelta(hours=20) # using for testing purpose

#queqelog_query = { "next_run_time": { "$lte": current_datetime } }
queqelog_query = {"$and": [{"next_run_time": { "$lte": current_datetime }}, {"executed": False}]}
queqelog_docs = collection_queue_log.find(queqelog_query).sort("next_run_time").limit(50)

for log_doc in queqelog_docs:
	th = Thread(target=runScrap, args=(log_doc,collection_queue_log))
	th.start()
