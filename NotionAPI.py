import requests
from dotenv import load_dotenv
import os
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm

# Load environment variables from the .env file
load_dotenv()

# Get the API token and database ID from environment variables
NOTION_API_TOKEN = os.getenv("NOTION_API_TOKEN")
NOTION_DATABASE_ID_DASHBOARD = os.getenv("NOTION_DATABASE_ID_DASHBOARD")
NOTION_DATABASE_ID_REF = os.getenv("NOTION_DATABASE_ID_REF")

# Set up the headers for the API request
headers = {
    "Authorization": f"Bearer {NOTION_API_TOKEN}",
    "Notion-Version": "2021-08-16",
    "Content-Type": "application/json",
}

class NotionAPI:
    def __init__(self, NOTION_API_TOKEN, NOTION_DATABASE_ID_DASHBOARD, NOTION_DATABASE_ID_REF):
        self.NOTION_API_TOKEN = NOTION_API_TOKEN
        self.NOTION_DATABASE_ID_DASHBOARD = NOTION_DATABASE_ID_DASHBOARD
        self.NOTION_DATABASE_ID_REF = NOTION_DATABASE_ID_REF
        self.headers = {
            "Notion-Version": "2022-06-28",
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": f"Bearer {NOTION_API_TOKEN}",
        }
        self._get_ref_df()
        self._get_dashboard_df()

        
    def _get_ref_df(self):
        self.ref_df = self.get_df(self.NOTION_DATABASE_ID_REF)
        self.ref_df['activity'] = self.ref_df['properties.Productions.title'].apply(lambda t: t[0]['text']['content'])
        self.ref_df = self.ref_df[['id','activity','properties.P_PC.select.name','properties.difficulty1-5.number']]
        print('ref_df loaded')
        
    def _get_dashboard_df(self):
        self.dashboard_df = self.get_df(self.NOTION_DATABASE_ID_DASHBOARD)
        self.dashboard_df['created_time'] = pd.to_datetime(self.dashboard_df['created_time'])
        print('dashboard loaded')

    def get_df(self,NOTION_DATABASE_ID):
        payload = {}

        response = requests.post(
            f"https://api.notion.com/v1/databases/{NOTION_DATABASE_ID}/query",
            headers=self.headers,
            json=payload,
        )

        if response.status_code == 200:
            data = response.json()
            result_df = pd.json_normalize(data['results'])
            return result_df
        else:
            print(f"Failed to retrieve data. Status code: {response.status_code}")
            print(response.text)
            return None

    def get_page_data(self, page_id):
        response = requests.get(f"https://api.notion.com/v1/blocks/{page_id}/children", headers=self.headers)

        if response.status_code == 200:
            page_data = response.json()
            return page_data
        else:
            print(f"Failed to retrieve page data. Status code: {response.status_code}")
            print(response.text)
            return None
        
    def _get_daily_check(self,page_id,created_time):
        try:
            blocks = self.get_page_data(page_id)['results']
            daily_check = self.get_df(blocks[0]['id'])
            daily_check['created_time'] = created_time 
            daily_check['ref_id'] = daily_check['properties.Activity.relation'].apply(lambda x: x[0]['id'])
            return daily_check[['created_time','ref_id','properties.Yes.checkbox']]
        except Exception as e:
            print(f'Page {page_id} date {created_time} got error:')
            print(e)
            return pd.DataFrame()
    
    def get_daily_checks(self, ids, created_times, workers=4):
        with ThreadPoolExecutor(max_workers=workers) as executor:
            results = list(tqdm(executor.map(self._get_daily_check, ids, created_times), total=len(ids)))
        return results

    def get_everything(self, workers=5):
        daily_checks = self.get_daily_checks(self.dashboard_df.id, self.dashboard_df.created_time, workers=workers)
        df_daily_checks =  pd.concat(daily_checks) 
        df_daily_checks = df_daily_checks.merge(
                        self.ref_df,
                        left_on=['ref_id'],
                        right_on=['id'] 
                    )
        return df_daily_checks
    
    
    
    
    
if __name__ == '__main__':
    notion_api = NotionAPI(NOTION_API_TOKEN, NOTION_DATABASE_ID_DASHBOARD, NOTION_DATABASE_ID_REF)
    df_all = notion_api.get_everything(10)
    df_all.to_csv('all_activity.csv')