from pymongo import MongoClient
from datetime import datetime, timedelta
from bson.objectid import ObjectId
import requests
import json


#client = MongoClient('localhost',27017)
client = MongoClient('mongodb+srv://admin:aquapi22@aquapi.geny1.mongodb.net/review_analytics')
db = client.review_analytics
collection_email_notfs = db.email_notifications
collection_email_notfs_slog = db.email_notifications_sentlog

#current_datetime = datetime.datetime.now() # getting local time
current_datetime = datetime.utcnow() # getting UTC time
yesterday_datetime = current_datetime - timedelta(hours=24)
day7ago_datetime = current_datetime - timedelta(hours=7*24)

email_notfs_query = {}
email_notfs_docs = collection_email_notfs.find(email_notfs_query)
for email_notfs_doc in email_notfs_docs:
	review_emails = email_notfs_doc['review_emails']
	team_id = email_notfs_doc['team_id']
	
	if review_emails == 'DAILY':
		param_feedback_time_span = 'Daily'
		param_startdate = yesterday_datetime.strftime("%Y-%m-%d")
		param_enddate = yesterday_datetime.strftime("%Y-%m-%d")

		sentlog_query = {"$and": [{"sent_at_utctime": { "$gt": yesterday_datetime }},{"sent_at_utctime": { "$lt": current_datetime }}, {"team_id": team_id}]}
		sentlog_results = collection_email_notfs_slog.find_one(sentlog_query)
		#print(sentlog_results)
		if sentlog_results:
			run_send_feedback_api = False
		else:
			run_send_feedback_api = True

	elif review_emails == 'WEEKLY':	
		param_feedback_time_span = 'Weekly'
		param_startdate = (day7ago_datetime).strftime("%Y-%m-%d")
		param_enddate = yesterday_datetime.strftime("%Y-%m-%d")

		sentlog_query = {"$and": [{"sent_at_utctime": { "$gt": day7ago_datetime }},{"sent_at_utctime": { "$lt": current_datetime }}, {"team_id": team_id}]}
		sentlog_results = collection_email_notfs_slog.find_one(sentlog_query)
		#print(sentlog_results)
		if sentlog_results:
			run_send_feedback_api = False
		else:
			run_send_feedback_api = True

	else:
		run_send_feedback_api = False


	if run_send_feedback_api == True:
		data = {
			"team_id":str(team_id),
			"feedback_time_span":param_feedback_time_span,
			"start_date":param_startdate,
			"end_date":param_enddate
		}
		response = requests.post(url='http://apiaquapi.101logix.com/api/feedback/sendFeedbackUpdates', json=data)
		response_json = response.json()
		is_success = response_json.get('success')

		if is_success == True:
			sent_log = {
				"team_id": team_id,
				"feedback_time_span": param_feedback_time_span,
				"start_date": param_startdate,
				"end_date": param_enddate,
				"sent_at_utctime": datetime.utcnow()
			}
			collection_email_notfs_slog.insert_one(sent_log)
