import streamlit as st

import pandas as pd
import numpy as np

from io import BytesIO

import requests
import base64
import time

from datetime import datetime
from zoneinfo import ZoneInfo
from dateutil import parser
from dateutil.relativedelta import relativedelta

the_start = time.time()

# load_dotenv()
# api_key = os.getenv('HARVEST_API_KEY')
# base64_api_key = base64.b64encode(f'{api_key}:'.encode()).decode()
# headers = { "Authorization": f"Basic {base64_api_key}", "Content-Type": "application/json" }

# Used to restrict data retrieved
lookback_months = 3

st.title('Extract Greenhouse Recruiting Data')

def click_button():
    st.session_state.button = True
    st.session_state.clicks += 1

# Functions for extracting greenhouse data
def get_starting_point(address,headers,history=20):
    response = requests.get(address,headers=headers)
    response.raise_for_status()
    n_pages = response.links['last']['url'].split('?')[-1].split('&')[0].split('=')[-1]
    if n_pages.isdigit():
        if int(n_pages) < history:
            return address
        else:
            start_here = int(n_pages)-history
            return f'{address}?page={start_here}&per_page=100'
    else:
        return address
    
def get_list(endpoint,headers,history=None,check_dates=False):
    # clock_start = time.time()
    output = []
    if history:
        my_endpoint = get_starting_point(endpoint,headers,history)
    else:
        my_endpoint = endpoint
    while my_endpoint:
        response = requests.get(my_endpoint,headers=headers)
        response.raise_for_status()
        data = response.json()
        if check_dates:
            min_date = get_datetime(data[0]['created_at'])
            max_date = get_datetime(data[0]['created_at'])
            for datum in data:
                min_date = min(min_date,get_datetime(datum['created_at']))
                max_date = max(max_date,get_datetime(datum['created_at']))
            # print(min_date,max_date)
        output.extend(data)
        if 'next' in response.links.keys():
            # print(response.links['next']['url'])
            my_endpoint = response.links['next']['url'] 
        else:
            break
    # clock_stop = time.time()
    # time_taken = clock_stop - clock_start
    #st.text('Collected {} records. Time taken for retrieval: {:.0f} seconds'.format(len(output),time_taken))
    return output

def office_location(x):
    if 'chicago' in x.lower():
        return 'CHI'
    if 'minn' in x.lower():
        return 'MSP'
    if 'atlanta' in x.lower():
        return 'ATL'
    if 'kosovo' in x.lower():
        return 'KOS'
    if 'detroit' in x.lower():
        return 'DET'
    if 'macedonia' in x.lower():
        return 'MAC'
    if 'dallas' in x.lower():
        return 'DAL'
    return 'UNKNOWN'

def job_location(x):
    
    prefix = x[:3]
    if 'archive' in x.lower():
        return None
    if prefix in ['KOS','CHI','ATL','MSP','ALB','DET','DAL','DFW']:
        if prefix == 'DFW':
            return 'DAL'
        return prefix
    
    return None

def get_jobs_dataframe(g_jobs):
    job_list = {
        'job_id':[],
        'job_name':[],
        # 'recruiter_id':[],
        # 'recruiter_name':[],
        # 'coordinator_id':[],
        # 'coordinator_name':[],
        'office':[],
        'department':[]
    }   
    for j in g_jobs:
        # Job status options: closed, draft, open. Take 'open' only
        if j['status'].lower() == 'open':
            job_list['job_id'].append(j['id'])
            job_list['job_name'].append(j['name'])
            job_list['office'].append(j['offices'][0]['name'])
            job_list['department'].append(j['departments'][0]['name'])
            # recruiter_id = None
            # recruiter_name = None
            # for r in j['hiring_team']['recruiters']:
            #     if r['responsible']:
            #         recruiter_id = r['id']
            #         recruiter_name = r['name']
            #         break
            # job_list['recruiter_id'].append(recruiter_id)
            # job_list['recruiter_name'].append(recruiter_name)
            # coordinator_id = None
            # coordinator_name = None
            # for coord in j['hiring_team']['coordinators']:
            #     if coord['responsible']:
            #         coordinator_id = coord['id']
            #         coordinator_name = coord['name']
            #         break
            # job_list['coordinator_id'].append(coordinator_id)
            # job_list['coordinator_name'].append(coordinator_name)
            
    df_jobs = pd.DataFrame().from_dict(job_list).dropna().reset_index(drop=True)
    df_jobs['location'] = df_jobs['office'].apply(office_location)
    return df_jobs

def get_stages_dataframe(g_stages):
    stage_list = {
        'stage_id':[],
        'stage_name':[],
        'interview_id':[],
        'interview_name':[],
        'job_id':[]
    }
    valid_jobs = np.unique(df_jobs['job_id'])
    for stg in g_stages:
        if stg['job_id'] in valid_jobs:
            if 'interviews' in stg.keys():
                for interview in stg['interviews']:
                    stage_list['stage_id'].append(stg['id'])
                    stage_list['stage_name'].append(stg['name'])
                    stage_list['interview_id'].append(interview['id'])
                    stage_list['interview_name'].append(interview['name'])
                    stage_list['job_id'].append(stg['job_id'])
    df_stage = pd.DataFrame().from_dict(stage_list).sort_values(by=['stage_id','interview_id'])
    return df_stage

def get_candidates_dataframe(g_candidates,valid_apps):
    candidate_list = {
        'candidate_id':[],
        'candidate_name':[],
        'created':[],
        'updated':[],
        'recruiter_name':[],
        'coordinator_name':[],
        'current_company':[],
        'application_id':[],
        'org_level':[]
    }

    for cand in g_candidates:
        if 'applications' in cand.keys():
            for app in cand['applications']:
                if (not app['prospect']) and (app['id'] in valid_apps):
                    candidate_list['candidate_id'].append(cand['id'])
                    candidate_list['candidate_name'].append(f"{cand['first_name']} {cand['last_name']}")
                    candidate_list['created'].append(parser.isoparse(cand['created_at']).astimezone(ZoneInfo('America/Chicago')))
                    candidate_list['updated'].append(parser.isoparse(cand['last_activity']).astimezone(ZoneInfo('America/Chicago'))) 
                    candidate_list['current_company'].append(cand['company'])
                    candidate_list['application_id'].append(app['id'])
                    if cand['recruiter'] is not None:
                        candidate_list['recruiter_name'].append(cand['recruiter']['name'])
                    else:
                        candidate_list['recruiter_name'].append(None)
                    if cand['coordinator'] is not None:
                        candidate_list['coordinator_name'].append(cand['coordinator']['name'])
                    else:
                        candidate_list['coordinator_name'].append(None)
                    
                    org_level = 'Unknown'
                    if 'organizational_level' in cand['custom_fields'].keys():
                        org_level = cand['custom_fields']['organizational_level']
                        if org_level is None:
                            org_level = 'Unspecified'
                            
                    candidate_list['org_level'].append(org_level)
                    
    df_cand = pd.DataFrame().from_dict(candidate_list).sort_values(by=['candidate_id','application_id']).reset_index(drop=True)
    return df_cand

def get_applications_dataframe(g_applications):
    app_list = {'application_id':[],
                'candidate_id':[],
                'updated':[],
                'job_id':[],
                'job_name':[],
                'status':[],
                'source':[],
                'stage':[]}
    for app in g_applications:
        skip = False
        for k in ['id','prospect','rejected_at','current_stage','jobs']:
            if k not in app.keys():
                skip = True
                break
        if not skip:
            if (not app['prospect']) and \
                (app['current_stage'] is not None) and \
                (app['jobs'] is not None) and \
                (app['rejected_at'] is None):
                
                # Check for an archived job
                job_ids = []
                job_names = []
                for j in app['jobs']:
                    if 'archive' not in j['name'].lower():
                        job_ids.append(j['id'])
                        job_names.append(j['name'])
                    
                if len(job_ids):
                    app_list['application_id'].append(app['id'])
                    app_list['candidate_id'].append(app['candidate_id'])
                    app_list['updated'].append(parser.isoparse(app['last_activity_at']).astimezone(ZoneInfo('America/Chicago')))

                    if len(job_ids) > 1:
                        print('[ERROR] Found multiple job IDs: ',app['id'])
                    app_list['job_id'].append(job_ids[0])
                    app_list['job_name'].append(job_names[0])
                    app_list['status'].append(app['status'])
                    app_list['stage'].append(app['current_stage']['name'])

                    if app['source'] is not None:
                        app_list['source'].append(app['source']['public_name'])
                    else:
                        app_list['source'].append(None)

    df_app = pd.DataFrame().from_dict(app_list)
    df_app['location'] = df_app['job_name'].apply(job_location)
    return df_app

def get_interviews_dataframe(g_interviews,valid_apps):
    interview_list = {
        'interview_id':[],
        'application_id':[],
        'interview_date':[],
        'interviewers':[],
        'type':[]
    }

    for interview in g_interviews:
        if 'application_id' in interview.keys():
            if interview['application_id'] in valid_apps:
                interview_list['interview_id'].append(interview['id'])
                interview_list['application_id'].append(interview['application_id'])
                interview_list['interview_date'].append(get_datetime(interview['start']['date_time']).date())
                interview_list['type'].append(interview['interview']['name'])
                if 'interviewers' in interview.keys():
                    int_list = []
                    for person in interview['interviewers']:
                        int_list.append(person['name'])
                    sort_index = [i for i, x in sorted(enumerate([x.split()[-1] for x in int_list]), key=lambda x: x[1])]
                    interview_list['interviewers'].append([int_list[i] for i in sort_index])
                    
    df_interview = pd.DataFrame().from_dict(interview_list).sort_values(by=['interview_id','application_id']).reset_index(drop=True)
    return df_interview

def get_scorecards_dataframe(g_scorecards):
    scorecard_list = {
        'scorecard_id':[],
        'application_id':[],
        'interview':[],
        'scorecard_author':[],
        'overall_recommendation':[]
    }
    for sc in g_scorecards:
        scorecard_list['scorecard_id'].append(sc['id'])
        scorecard_list['application_id'].append(sc['application_id'])
        scorecard_list['interview'].append(sc['interview_step']['name'])
        scorecard_list['scorecard_author'].append(sc['interviewer']['name'])
        scorecard_list['overall_recommendation'].append(sc['overall_recommendation'])
    df_scorecard = pd.DataFrame().from_dict(scorecard_list)
    df_scorecard['overall_recommendation'].fillna('no_decision',inplace=True)

    # Grouping and sorting
    dfg_scorecard = df_scorecard.groupby(by=['application_id','interview'],as_index=False).agg(list)
    author_sorted, id_sorted, rec_sorted = [], [], []
    for _,row in dfg_scorecard.iterrows():
        idx_sorted = [i for i, x in sorted(enumerate([x.split()[-1] for x in row['scorecard_author']]), key=lambda x: x[1])]
        author_sorted.append([row['scorecard_author'][i] for i in idx_sorted])
        id_sorted.append([row['scorecard_id'][i] for i in idx_sorted])
        rec_sorted.append([row['overall_recommendation'][i] for i in idx_sorted])
        
    dfg_scorecard['scorecard_author'] = author_sorted
    dfg_scorecard['scorecard_id'] = id_sorted
    dfg_scorecard['overall_recommendation'] = rec_sorted

    return dfg_scorecard

def get_datetime(x):
    return parser.isoparse(x)

def to_excel(df):
    output = BytesIO()
    # writer = pd.ExcelWriter(output, engine='xlsxwriter')
    writer = pd.ExcelWriter(output)
    df.to_excel(writer, index=False, sheet_name='Results')
    #workbook = writer.book
    #worksheet = writer.sheets['Results']
    #format1 = workbook.add_format({'num_format': '0.00'}) 
    #worksheet.set_column('A:A', None)  
    writer.close()
    processed_data = output.getvalue()
    return processed_data

# Endpoint setup
start_time     = (datetime.now()-relativedelta(months=lookback_months)).isoformat()
job_start_time = (datetime.now()-relativedelta(months=12)).isoformat()

job_endpoint         = f'https://harvest.greenhouse.io/v1/jobs?per_page=500&updated_after={job_start_time}'
interview_endpoint   = f'https://harvest.greenhouse.io/v1/scheduled_interviews?per_page=500&updated_after={start_time}'
application_endpoint = f'https://harvest.greenhouse.io/v1/applications?per_page=500&last_activity_after={start_time}'
candidate_endpoint   = f'https://harvest.greenhouse.io/v1/candidates?per_page=500&updated_after={start_time}'
scorecard_endpoint   = f'https://harvest.greenhouse.io/v1/scorecards?per_page=500&updated_after={start_time}'
stages_endpoint      = f'https://harvest.greenhouse.io/v1/job_stages?per_page=500'

if 'button' not in st.session_state:
    st.session_state.button = False

if 'clicks' not in st.session_state:
    st.session_state.clicks = 0

#st.button('Click me!',on_click=click_button)
with st.form(key='my_form'):
    text_input_container = st.empty()
    api_key = text_input_container.text_input('Enter your Greenhouse API Key')
    valid_api_key = True
    run_extract = st.form_submit_button(label='Extract Data')
    if run_extract:
        if api_key == '':
            st.stop()
        else:
            base64_api_key = base64.b64encode(f'{api_key}:'.encode()).decode()
            headers = { "Authorization": f"Basic {base64_api_key}", "Content-Type": "application/json" }

            try:
                progress_bar = st.progress(0,text='Starting extraction...')
                # st.write('Extracting Jobs...')
                gh_jobs = get_list(endpoint=job_endpoint,headers=headers)
                df_jobs = get_jobs_dataframe(gh_jobs)
                text_input_container.empty()
                # st.progress(5,text='Job data extracted')
                # st.write('Extracting Job Stages...this will take several seconds')
                gh_stages = get_list(endpoint=stages_endpoint,headers=headers)
                df_stages = get_stages_dataframe(gh_stages)
                progress_bar.progress(20,text='Job data extracted')
                # st.write('Extracting Applications...this might take a minute')
                gh_applications = get_list(endpoint=application_endpoint,headers=headers)
                df_applications = get_applications_dataframe(gh_applications)
                progress_bar.progress(50,text='Application data extracted')
                # st.write('Extracting Candidates...this might take a minute')
                gh_candidates = get_list(endpoint=candidate_endpoint,headers=headers)
                df_candidates = get_candidates_dataframe(gh_candidates,valid_apps=set(df_applications['application_id']))
                progress_bar.progress(80,text='Candidate data extracted')
                # st.write('Extracting Interviews...')
                gh_interviews = get_list(endpoint=interview_endpoint,headers=headers)
                df_interviews = get_interviews_dataframe(gh_interviews,valid_apps=set(df_applications['application_id']))
                progress_bar.progress(85,text='Interview data extracted')
                # st.write('Extracting Scorecards...this will take a few seconds')
                gh_scorecards = get_list(endpoint=scorecard_endpoint,headers=headers)
                df_scorecards = get_scorecards_dataframe(gh_scorecards)
                progress_bar.progress(98,text='Data Extracted!')

                # Assemble constituents
                df = pd.merge(df_applications,df_candidates,
                            on=['application_id','candidate_id'],
                            suffixes=['_app','_cand'])
                df = pd.merge(df.drop(columns='job_name'),df_jobs,on=['job_id'],suffixes=['_app','_job'])
                df_int_scorecard = pd.merge(df_interviews,df_scorecards.rename(columns={'interview':'type'}),
                                            on=['application_id','type'],how='left')
                df_test = pd.merge(df,df_int_scorecard,on=['application_id'],suffixes=['_app','_int'])
                df_test['profile_url'] = df_test[['candidate_id','application_id']].apply(lambda x: f'https://app2.greenhouse.io/people/{x[0]}?application_id={x[1]}',axis=1)
                df_test = pd.merge(df_test,
                                df_stages[df_stages['stage_name'].isin(['Preliminary Phone Screen','Stage 1','Stage 2','Stage 3'])][['job_id','interview_name','stage_name']].rename(columns={'interview_name':'type'}),
                                on=['job_id','type'])
                
                df_test['updated_app'] = df_test['updated_app'].dt.tz_localize(None)
                df_test['updated_cand'] = df_test['updated_cand'].dt.tz_localize(None)
                df_test['created'] = df_test['created'].dt.tz_localize(None)

                progress_bar.progress(100,text='Dataframe prepared. Ready to download')
                # df_xlsx = to_excel(df_test)
                # progress_bar.progress(100,text='Excel file generated')
            except:
                st.write('Error extracting data. Try rerunning (re-enter your Harvest API Key if necessary)')

try:
    csv_output = df_test.to_csv(index=False).encode('utf-8')
    dl_button = st.download_button(label='Download Result as CSV',data=csv_output,file_name='Results.csv',mime='text/csv')
    # buffer = BytesIO()
    # with pd.ExcelWriter(buffer,engine='xlsxwriter') as writer:
    #     df_test.to_excel(writer,index=False,sheet_name='Results')
    #     dl_button = st.download_button(label='Download Result',data=buffer,file_name='df_test.xlsx')#,mime='application/vnd.ms-excel')
    # # st.download_button(label='Download Result',data=df_xlsx,file_name='df_test.xlsx')
except:
    pass
