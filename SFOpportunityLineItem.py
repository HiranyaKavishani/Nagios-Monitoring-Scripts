import datetime
import json
import MySQLdb
import os, sys
import requests,json

apiOpportunityLineItemCount = 0
syncDBOpportunityLineItemCount = 0

checkedIn = datetime.timedelta(seconds=24*60*60)
dateformat = "%Y-%m-%d %H:%M:%S.%f"
now = datetime.datetime.now()
path = ""
filename = "sfOppoLineInitialWarning.txt"

clientId = ""
grantType = "refresh_token"
refreshToken = ""
url = "https://login.salesforce.com/services/oauth2/token"
api_url = "https://wso2.my.salesforce.com/services/data/v43.0/query/?q=SELECT count() from OpportunityLineItem"

hostip = ""
username = ""
password =""
database = ""

dbQueryForRetrieveOppoLineItem = "SELECT DISTINCT count(*) FROM UnifiedDashboards.SF_OPPOLINEITEM where IsInSF = 1"

criticalMessage = "CRITICAL.Please check ASAP"
warningMessage = "WARNING.There are mis-matched records"
okMessage = "Ok.There are no any mis-matched records"
unknownMessage = "UnknownError"
dbError = "DB Error: unable to fecth oppoLineItem data"
apiAccessError = "Can't access the API"

task = {
 "client_id": clientId,
 "grant_type": grantType,
 "refresh_token":refreshToken
}

resp = requests.post(url,data=task,headers={'Content-Type': 'application/x-www-form-urlencoded'})

if resp.status_code != 200:
    print ("Error")
    sys.exit(3)

else:
    data = resp.json()
    access_token = data['access_token']
    api_call_headers = {'Authorization': 'Bearer ' + access_token}
    api_call_response = requests.get(api_url, headers=api_call_headers)

    if api_call_response.status_code != 200:
        print apiAccessError
        sys.exit(3)

    else:
        data = api_call_response.json()
        apiOpportunityLineItemCount = data['totalSize']


db = MySQLdb.connect(host=hostip, user=username, passwd=password, db=database)
cursor = db.cursor()
sql = dbQueryForRetrieveOppoLineItem;

try:
    cursor.execute(sql)
    results = cursor.fetchall()
    for row in results:
        syncDBOpportunityLineItemCount = row[0]
except:
    print dbError
    sys.exit(3)

db.close()

print(apiOpportunityLineItemCount)
print(syncDBOpportunityLineItemCount)

if (apiOpportunityLineItemCount == syncDBOpportunityLineItemCount):
    if os.path.isfile(path+filename) and os.path.getsize(path+filename) > 0:
        f = open(path+filename,'r+')
        f.truncate(0)
        f.close()
    print okMessage
    sys.exit(0)

elif apiOpportunityLineItemCount <> syncDBOpportunityLineItemCount:
    if os.path.isfile(path+filename) and os.path.getsize(path+filename) > 0:
        f = open(path+filename,'r+')
        data = f.read()
        oldDate = datetime.datetime.strptime(data,dateformat)
        diff = (now-oldDate)
        print(diff)
        print(checkedIn)
        if (diff >= checkedIn):
            print criticalMessage
            sys.exit(2)
    else:
        f = open(path+filename,'w+')
        f.write(str(now))

    f.close()
    print warningMessage
    sys.exit(1)

else:
    print unknownMessage
    sys.exit(3)
