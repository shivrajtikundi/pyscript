from pymongo import MongoClient
from datetime import datetime, timedelta
from bson.objectid import ObjectId
import requests
import json


#client = MongoClient('localhost',27017)
client = MongoClient('mongodb+srv://admin:aquapi22@aquapi.geny1.mongodb.net/review_analytics')
db = client.review_analytics
collection_user_licence = db.user_licences
collection_reviews = db.analysed_app_reviews

current_datetime = datetime.now() # getting local time
#current_datetime = datetime.utcnow() # getting UTC time


user_licence_query = {"$and": [{"billing_type":"ANNUALY"}, {"is_renewable":True}]}
user_licence_docs = collection_user_licence.find(user_licence_query)
for user_licence_doc in user_licence_docs:
	renewal_date = user_licence_doc['renewal_date']

	if renewal_date <= current_datetime:
		cycle_start_on = user_licence_doc['cycle_start_on']
		cycle_end_on = user_licence_doc['cycle_end_on']
		new_cycle_start_on = cycle_end_on + timedelta(seconds=1)
		new_cycle_end_on = new_cycle_start_on + timedelta(hours=24*30) - timedelta(seconds=1)
		#print(new_cycle_start_on)
		#print(new_cycle_end_on)

		total_review_quota = user_licence_doc['review_max_count'] + user_licence_doc['review_carry_forwarded_max_count'] + user_licence_doc['extra_review_max_count']
		#print(total_review_quota)

		findrevw_query = {"$and": [{"created_on": { "$gte": cycle_start_on }},{"created_on": { "$lte": cycle_end_on }}, {"user_id": user_licence_doc['userid']}, {"team_id": user_licence_doc['team_id']}]}
		findrevw_docs = collection_reviews.find(findrevw_query)
		findrevw_count = len(list(findrevw_docs))
		#print(findrevw_count)

		review_remains = total_review_quota - findrevw_count
		if review_remains < 0:
			review_remains = 0
		#print(review_remains)

		new_licence = {
			"billing_type": user_licence_doc["billing_type"],
			"is_paid": user_licence_doc["is_paid"],
			"payment_id": user_licence_doc["payment_id"],
			"is_active": user_licence_doc["is_active"],
			"review_max_count": user_licence_doc["review_max_count"],
			"review_carry_forwarded_max_count": review_remains,
			"extra_review_max_count": 0,
			"is_renewable": user_licence_doc["is_renewable"],
			"userid": user_licence_doc["userid"],
			"team_id": user_licence_doc["team_id"],
			"licenceid": user_licence_doc["licenceid"],
			"start_on": user_licence_doc["start_on"],
			"end_on": user_licence_doc["end_on"],
			"cycle_start_on": new_cycle_start_on,
			"cycle_end_on": new_cycle_end_on,
			"renewal_date": new_cycle_end_on,
			"created_on": datetime.now(),
			"modified_on": datetime.now()
		}
		collection_user_licence.insert_one(new_licence)

		doquery = { "_id": user_licence_doc["_id"] }
		newvalues = { "$set": { "is_renewable":False } }
		collection_user_licence.update_one(doquery, newvalues)
