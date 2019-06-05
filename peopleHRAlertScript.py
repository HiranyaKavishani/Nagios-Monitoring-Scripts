import datetime
import json
import MySQLdb
import os, sys
import requests

apiEmployeeCount = 0
syncDBEmployeeCount = 0
checkedIn = datetime.timedelta(seconds=60)
now = datetime.datetime.now()
dateformat = "%Y-%m-%d %H:%M:%S.%f"
filename = "peopleHR.txt"
path = ""


apiKey = ""
action = ""
queryName = ""
peopleHRUrl = ""

hostip = ""
username = ""
password =""
database = ""
dbQueryForRetrieveActiveEmployee = "Select Count(*) from people_HR.PEOPLE_HR where  EmployeeStatus='Active' OR EmployeeStatus='Marked leaver'"

criticalMessage = "CRITICAL [PEOPLE_HR].Please check ASAP"
warningMessage = "WARNING [PEOPLE_HR].There are mis-matched records"
okMessage = "Ok [PEOPLE_HR].There are no any mis-matched records"
unknownMessage = "UnknownError [PEOPLE_HR]"
dbError = "[PEOPLE_HR] DB Error: unable to fecth data"
apiAccessError = "Can't access the [PEOPLE_HR] API"

task = {
 "APIKey": apiKey,
 "Action": action,
 "QueryName": queryName 
}

resp = requests.post(peopleHRUrl,json=task)
if resp.status_code != 200:
    print apiAccessError
    sys.exit(3)
    
else:
   data = resp.json()
   employeeData = data['Result']
   for y in employeeData:
          apiEmployeeCount = apiEmployeeCount + 1


db =MySQLdb.connect(host=hostip, user=username, passwd=password, db=database)
cursor = db.cursor()
sql = dbQueryForRetrieveActiveEmployee;

try:
   cursor.execute(sql)
   results = cursor.fetchall()
   for row in results:
      syncDBEmployeeCount = row[0]
except:
   print dbError
   sys.exit(3)

db.close()

#print(apiEmployeeCount)
#print(syncDBEmployeeCount)

if apiEmployeeCount == syncDBEmployeeCount:
    if os.path.isfile(path+filename) and os.path.getsize(path+filename) > 0:
        f = open(filename,'r+')
        f.truncate(0)
        f.close()    
    print okMessage
    sys.exit(0)
        
elif apiEmployeeCount <> syncDBEmployeeCount:
     if os.path.isfile(path+filename) and os.path.getsize(path+filename) > 0:
         f = open(path+filename,'r+')
         data = f.read()
         oldDate = datetime.datetime.strptime(data,dateformat)
         
         diff = (now-oldDate)
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





