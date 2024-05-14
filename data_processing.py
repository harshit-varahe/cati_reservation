import re
import  pandas as pd
def clean_text(text):
    without_hindi = re.split(r'[\u0900-\u097F]+', text, maxsplit=1)[0]
    without_specific_chars_test = without_hindi.split('|')[0].strip()
    without_multiple_spaces = re.sub(r'\s{2,}', ' ', without_specific_chars_test)
    return without_multiple_spaces
def data_split(calling_report_df , state=None):
    if not calling_report_df.empty:
        campaign_info= calling_report_df['campaignID'].str.extract(r'CATI_([\w&]+)_RESERVATION_R(\d+)', expand=True)
        calling_report_df['state_abb'] = campaign_info[0]
        calling_report_df['election_round'] = campaign_info[1]
        calling_report_df['election_cycle'] = f'{state}_2024'
        calling_report_df[['agentName', 'agentID']] = calling_report_df['agentId'].str.split('-', expand=True)
        calling_report_df[['date', 'time']] = calling_report_df['callDate'].str.split('T', expand=True)
        calling_report_df['time'] = calling_report_df['time'].str.split('.').str[0]
        calling_report_df['customerName'] = calling_report_df['customerName'].apply(clean_text)
        calling_report_df.drop(columns=["companyName", "callDate", "campaignName", "agentId", "agentMobileNo"],
                               inplace=True)
    return calling_report_df
def question_cleaning(questionset_df, calling_report_df):
    """
    Clean and process questions in DataFrames.
    This function cleans and processes questions in 'questionset_df' and 'calling_report_df'
    for analysis or reporting.
    Parameters:
    - questionset_df (pandas.DataFrame): DataFrame with questions.
    - calling_report_df (pandas.DataFrame): DataFrame with calling report data.

    """
    keys_to_drop = []
    for i in range(1, len(questionset_df.columns) + 1):
        q_key = f"q{i}"
        question_key = f"question{i}"
        questionset_df[q_key] = questionset_df[question_key].str.split("|").str[-1].str.strip()
        if not pd.isnull(questionset_df[q_key].iloc[0]):
            calling_report_df[q_key] = calling_report_df[q_key].str.split("|").str[-1].str.strip()
            try:
                calling_report_df[questionset_df[q_key].iloc[0]] = calling_report_df[q_key]
            except Exception as e:
                print(f"An error occurred: {e}")
        else:
            keys_to_drop.append(q_key)
        calling_report_df.drop(columns=[q_key], inplace=True)
    return questionset_df, calling_report_df
def key_mapping(calling_report_data):
    calling_report_data['sync_date'] = pd.to_datetime('today').strftime('%Y-%m-%d')
    calling_report_data['sync_time'] = pd.to_datetime('now').strftime('%H:%M:%S')
    calling_report_data.fillna('', inplace=True)
    return calling_report_data
def rejection_criteria(calling_report_data):
    calling_report_data[["time_validation","raw","v2_rejection","mandatory_ques_flag"]] = [True,1, True,True]
    mandatory_columns = [
        "congress_reservation_propaganda"
        ]
    calling_report_data["totalDuration"] = calling_report_data["totalDuration"].replace('',0)
    calling_report_data["talkDuration"] = calling_report_data["talkDuration"].replace('',0)
    calling_report_data["totalDuration"] = calling_report_data["totalDuration"].astype(int)
    calling_report_data["talkDuration"] = calling_report_data["talkDuration"].astype(int)
    # calling_report_data["time_validation"] = calling_report_data["talkDuration"] != 0
    calling_report_data["mandatory_ques_flag"] = ~calling_report_data[mandatory_columns].apply(
        lambda row: any(pd.isna(x) or x == "" for x in row), axis=1)
    calling_report_data["v1_rejection"] = (
            calling_report_data["time_validation"] & calling_report_data["mandatory_ques_flag"]).astype(bool)
    calling_report_data["final_rejection"] = (calling_report_data["v1_rejection"] & calling_report_data[
        "v2_rejection"]) | False
    print('preprocessing done')
    return calling_report_data