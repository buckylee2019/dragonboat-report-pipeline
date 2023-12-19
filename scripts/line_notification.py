import pandas as pd
import requests
from datetime import date
import json
from dotenv import load_dotenv
import os
load_dotenv()


# DATA_DIR="/Users/buckylee/Library/CloudStorage/Box-Box/My Box/99_Tools/Airflow/scripts/data/"
DATA_DIR = "/opt/airflow/scripts/data/"

def remaining(row):
    return row['TOTAL_AMOUNT'] - row['PAID_AMOUNT']

def construct_msg(row):
    message = f"*{row['ENGNAME']}* 須繳納 ${round(row['remaining'])}"
    return message


def send_notification(notificaiton):
    url = os.environ.get('LINE_API_ENDPOINT')

    payload = json.dumps({
        "text": notificaiton
    })
    headers = {
        'Content-Type': 'application/json',
    }


    response = requests.request("POST", url, headers=headers, data=payload)
    return response.json()
df = pd.read_csv(DATA_DIR+'financial_report.csv')

df.fillna(0,inplace=True)
df['remaining'] = df.apply(remaining,axis=1)
df = df.drop(df[df['remaining'] <= 1].index)

df_an = pd.read_csv(DATA_DIR+'announcement.csv')

today = date.today().strftime("%Y-%m-%d")
df_an['Date'] = pd.to_datetime(df_an['Date'],format='%Y-%m-%d')
df_an = df_an[df_an['Date']==today]
announcement = '\n'.join(list(df_an['Announce']))
print(df_an)
print(today)

if len(df['ENGNAME'])<=1 :
    if announcement != "":
        info = f"`今日公告事項：` \n {announcement}"

        send_notification(info)
        print('Sent announcement')
    print('No unpaid expense.')
        # print(info)

else:
    df['ENGNAME'] = df.apply(construct_msg,axis=1)
    df = df.sort_values('ENGNAME')
    info = '早安，\n以下名單有未結清帳款：\n\n'+'\n'.join(list(df['ENGNAME']))+'\n\n詳細帳目請查看: https://lookerstudio.google.com/s/rmC_pQdxEqk\n\n'+"請轉帳至💳：\n\n013 國泰世華 \n699509391509"
    if announcement!="":
        info += f"`今日公告事項：` \n{announcement}"
    print(info)
    send_notification(info)

