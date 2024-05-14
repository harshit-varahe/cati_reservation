import pymongo
from pymongo import UpdateOne
import time
def push_data_to_mongodb( data=None):
    print("now pushing data to mongo")
    start_time = time.time()
    # for local host testing purpose
    # client = pymongo.MongoClient("mongodb://localhost:27017")
    # db = client["CATI_RAW_RESPONSE"]
    # collection = db["vb"]
    client = pymongo.MongoClient("mongodb+srv://harshit_varahe:iq3GbTAnawR551G0@cati.hoamu.mongodb.net/")
    db = client["cati_central"]
    collection = db["CATI_RESERVATION_raw_response"]
    # collection = db["CATI_testing"]
    bulk_write_request_newresponse = []
    for one_response in data:
        # print(one_response)
        bulk_write_request_newresponse.append(
            UpdateOne({"state_abb": one_response["state_abb"],
                        'uniqueID': one_response['uniqueID']},
                        {"$set": one_response}, upsert=False))
        # break
    collection.bulk_write(bulk_write_request_newresponse) # for local host testing purpose
    print(f"data pushed , time takem = {time.time() - start_time}")
    client.close()
    return "done"

