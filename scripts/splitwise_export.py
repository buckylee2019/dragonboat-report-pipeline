# import splitwise
from splitwise import Splitwise
from datetime import datetime,timedelta
from airflow.models import Variable
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv
import os
load_dotenv()


api_key = os.environ.get('SPLIT_API_KEY')



sObj = Splitwise("<consumer key>","<consumer secret>",api_key = api_key)
user = sObj.getCurrentUser()
userid = user.getId()
groups = sObj.getGroups()


date_export = Variable.get('date_export', default_var=datetime.now().strftime('%Y-%m-%d'))
# date_export = "2023-06-17"
timedelta(1)
dated_before = datetime.strptime(date_export, '%Y-%m-%d') + timedelta(days=1)
dated_after = dated_before - relativedelta(months=1)

# 帳戶,幣種,記錄類型,主類別,子類別,金額,手續費,折扣,名稱,商家,日期,時間,專案,描述,標籤,對象
expense = sObj.getExpenses(dated_after=dated_after, dated_before=dated_before,limit=60)
# data_dir = "/Users/buckylee/Library/CloudStorage/Box-Box/My Box/99_Tools/Airflow/scripts/data/splitwise_2023-06-02.csv"
data_dir = f"/opt/airflow/scripts/data/splitwise_{date_export}.csv"
with open(data_dir,'w') as f:
    f.write("帳戶,幣種,記錄類型,主類別,子類別,金額,手續費,折扣,名稱,商家,日期,時間,專案,描述,標籤,對象\n")
    print(f"Number of expense:{len(expense)}")
    for ex in expense:
        print(ex.getCategory().getName())
        if ex.getCategory().getName() == 'Dining out':
            category = "飲食"
            subcategory = "晚餐"
        elif ex.getCategory().getName() == 'Food and drink - Other':
            category = "飲食"
            subcategory = "酒類"
        elif ex.getCategory().getName() == 'Taxi':
            category = "交通"
            subcategory = "計程車"
        elif ex.getCategory().getName() == 'Clothing':
            category = "購物"
            subcategory = "衣物"
        elif ex.getCategory().getName() == 'Gas/fuel':
            category = "交通"
            subcategory = "加油費"
        elif ex.getCategory().getName() ==  "Taxes":
            category = "個人"
            subcategory = "稅金"
        else:
            category = "購物"
            subcategory = "市場"
        amount = []
        owed = []
        paidbycurrent = False
        
        for payment in ex.getRepayments():
            if payment.getToUser() ==  userid and ex.getCost() == payment.getAmount() or userid not in [payment.getFromUser(),payment.getToUser()]:
                print('No Expense')
                continue
            if payment.getToUser() ==  userid:
                paidbycurrent = True
            else:
                paidbycurrent = False
            amount.append(float(payment.getAmount()))
           
            paidBy = sObj.getUser(payment.getToUser()).getFirstName()
            owed.append(sObj.getUser(payment.getFromUser()).getFirstName())
        if len(amount)==0:
            continue
        detail = f"Splitwise expense: - Original Cost: {ex.getCost()}, Paid by {paidBy}, split by {'/'.join(owed)}/{paidBy}"
        if paidbycurrent:
            paidamount = float(ex.getCost()) - sum(amount)
        else:
            paidamount = sum(amount)
        dt = datetime.fromisoformat(ex.getDate()[:-1])
        date = dt.date()
        time = dt.time()

        # Convert date and time components to string
        date_str = date.isoformat()
        time_str = time.isoformat()
        f.write(f"錢包,TWD,支出,{category},{subcategory},-{paidamount},0,0,{ex.getDescription()},,{date_str},{time_str},,{detail},,")
        f.write("\n")
# for g in groups:
#     print(g.getId(),g.getName())
#     print(g.getMembers())