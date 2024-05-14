
import requests
import pandas as pd
from datetime import datetime
import numpy as np
import time
import data_processing
import mongo_ops
import pymongo
import traceback

url = "https://www.itbyvi.in/gcapi/api/VICalling/CallingReport"

states = ['UKD','MH','HP', 'KA', 'PB','JnK', 'JH', 'HR', 'CT', 'AP']

for state in states:
    print('state - ',  state)
    campaign_id = f'CATI_{state}_RESERVATION_R1'
    fromdate = datetime.now().strftime("%Y-%m-%d")
    todate = datetime.now().strftime("%Y-%m-%d")
    headers = {
        "Authorization": "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJodHRwOi8vc2NoZW1hcy54bWxzb2FwLm9yZy93cy8yMDA1LzA1L2lkZW50aXR5L2NsYWltcy9uYW1lIjoiR3JlYW4gQ2FsbCIsImh0dHA6Ly9zY2hlbWFzLnhtbHNvYXAub3JnL3dzLzIwMDUvMDUvaWRlbnRpdHkvY2xhaW1zL2VtYWlsYWRkcmVzcyI6Ijc1NTcyMjIzMzMiLCJodHRwOi8vc2NoZW1hcy5taWNyb3NvZnQuY29tL3dzLzIwMDgvMDYvaWRlbnRpdHkvY2xhaW1zL3JvbGUiOiJBZG1pbiIsImp0aSI6ImI0Y2MyZTgxLTEyZDQtNGQ1ZS05MmNiLTEyYWMyYjdlNmQzNyIsImV4cCI6MTg1MTg3MTM2MCwiaXNzIjoiaHR0cHM6Ly9ncmVlbmNhbGwuaW4vIiwiYXVkIjoiaHR0cHM6Ly9ncmVlbmNhbGwuaW4vIn0.4wt1cxtR6926IA4RrhO9yC7djs5M5gbnytsb6jKrCMk"
    }
    calling_report_results = []
    questionset_results = []
    # data = {"fromdate": fromdate, "todate": todate, "campaignId": campaign_id}
    data = {"fromdate": "2024-05-06", "todate": "2024-05-09", "campaignId": campaign_id}
    print(data)
    try:
        start_time = time.time()
        response = requests.post(url, json=data, headers=headers, stream=True)
        print(response)
        if response.status_code == 200:
            result = response.json()
            callingreport = result["result"]["callingreport"]
            questionset = result["result"]["questionset"]
            print(campaign_id, len(callingreport))
            calling_report_results.extend(callingreport)
            questionset_results.extend(questionset)
        calling_report_df = pd.DataFrame(calling_report_results)
        questionset_df = pd.DataFrame(questionset_results)
        print(f"data pulled from api , time takem = {time.time() - start_time}")
        splitted_df = data_processing.data_split(calling_report_df , state=state)
        _, cleaned_calling_report_data = data_processing.question_cleaning(questionset_df, splitted_df)
        calling_report_data = data_processing.key_mapping(cleaned_calling_report_data)
        print(calling_report_data.columns)
        CRD_after_rejection=data_processing.rejection_criteria(calling_report_data)
        data = CRD_after_rejection.to_dict(orient='records')
        mongo_ops.push_data_to_mongodb(data=data)
    except Exception as e:
        print(e)
        print(traceback.format_exc())


