from pymongo import MongoClient
from threading import Thread
from datetime import datetime
import requests
import json

def runAnalysis(userdoc,col_log):
	data = {
		"user_id":str(userdoc["_id"]),
		"team_id":str(userdoc["team_id"]),
		"limit":"20"
	}
	response = requests.post(url='https://py.reviewanalytics.io/analysis', json=data)
	response_json = response.json()
	get_status_code = response_json.get('status_code')
	
	if get_status_code == 200:
		total_execution_time = response_json.get('data').get('total_execution_time')
		run_log = {
			"user_id": userdoc["_id"],
			"team_id": userdoc["team_id"],
			"total_execution_time": total_execution_time,
			"created_at": datetime.now()
		}
		col_log.insert_one(run_log)


#client = MongoClient('localhost',27017)
client = MongoClient('mongodb+srv://admin:aquapi22@aquapi.geny1.mongodb.net/review_analytics')
db = client.review_analytics
collection_users = db.user_masters
collection_analysis_rlog = db.analysis_runlog

users_query = {"$and": [{"is_active": True}, {"user_state": "ACTIVE"}]}
user_docs = collection_users.find(users_query).sort("last_logged_in_time",-1)

for user_doc in user_docs:
	th = Thread(target=runAnalysis, args=(user_doc,collection_analysis_rlog,))
	th.start()
