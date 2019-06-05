import datetime
import json
import MySQLdb
import os, sys
import requests,json
from lxml import etree

apiInvoiceCount = 0
syncDBAccountCount = 0

checkedIn = datetime.timedelta(seconds=60)
now = datetime.datetime.now()
dateformat = "%Y-%m-%d %H:%M:%S.%f"
path = ""
filename = "netsuiteInvoiceWarning.txt"
netsuiteDiffFile = "netsuiteDiff.txt"

api_url = "https://webservices.netsuite.com/services/NetSuitePort_2014_1"

hostip = "192.168.56.228"
username = "root"
password = "cioroot"
database = "UnifiedDashboards"
startDate = '2018-07-01'
endDate = '2018-09-31'

dbQueryForRetrieveInvoiceCount = "SELECT count(*) from NTST_Invoices WHERE createdDate BETWEEN '2018-07-01' AND '2018-09-31'"
dbQueryForRetrieveInvoiceIdList = "SELECT InvoiceId from NTST_Invoices WHERE createdDate BETWEEN '2018-07-01' AND '2018-09-31'"

criticalMessage = "CRITICAL [NTST_INVOICE].Please check ASAP."
warningMessage = "WARNING [NTST_INVOICE].There are mis-matched records."
okMessage = "Ok [NTST_INVOICE].There are no any mis-matched records."
unknownMessage = "UnknownError [NTST_INVOICE]"
dbError = "DB Error: unable to fecth account data from [NTST_INVOICE]"
apiAccessError = "Can't access the Netsuite API"

payload ='''<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:urn="urn:messages_2014_1.platform.webservices.netsuite.com" xmlns:urn1="urn:core_2014_1.platform.webservices.netsuite.com" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
   <soapenv:Header>
      <urn:searchPreferences>
         <!--Optional:-->
         <urn:bodyFieldsOnly>true</urn:bodyFieldsOnly>
         <!--Optional:-->
         <urn:returnSearchColumns>true</urn:returnSearchColumns>
         <!--Optional:-->
         <urn:pageSize></urn:pageSize>
      </urn:searchPreferences>
      <urn:partnerInfo>
         <!--Optional:-->
         <urn:partnerId></urn:partnerId>
      </urn:partnerInfo>
      <urn:applicationInfo>
         <!--Optional:-->
         <urn:applicationId>A58B6E15-8CFE-4D85-A5C7-EC94FD253589</urn:applicationId>
      </urn:applicationInfo>
       <urn:passport>
         <urn1:email>techops@wso2.com</urn1:email>
         <urn1:password>Welcome!@23#?DigiOps</urn1:password>
         <urn1:account>3883026_SB1</urn1:account>
         <!--Optional:-->
         <urn1:role internalId="" externalId="" type="">
            <!--Optional:-->
            <urn1:name></urn1:name>
         </urn1:role>
      </urn:passport>
   </soapenv:Header>
   <soapenv:Body>
      <urn:search>
         <searchRecord xmlns:q1="urn:sales_2014_1.transactions.webservices.netsuite.com"
xsi:type="q1:TransactionSearchAdvanced" savedSearchId="803" />
      </urn:search>
   </soapenv:Body>
</soapenv:Envelope>'''

nsmap = {'platformCore': 'urn:core_2014_1.platform.webservices.netsuite.com'}
nsmap2 ={'platformCommon': 'urn:common_2014_1.platform.webservices.netsuite.com'}
invoiceAPIList = []
invoiceDBList = []
connecterList = []
found = 0

resp = requests.post(api_url,data=payload,headers={'Content-Type': 'application/xml', 'SOAPAction' : 'search'})
if resp.status_code != 200:
    print ("API Access Error")
    sys.exit(3)

else:
    root = etree.fromstring(resp.content)
    apiInvoiceCount = root.findall('.//platformCore:totalRecords', nsmap)[0].text
    data = root.findall('.//platformCommon:tranId', nsmap2)
    for value in data:
        dataval = value.findall('.//platformCore:searchValue', nsmap)[0]
        invoiceAPIList.append(dataval.text)
    

db = MySQLdb.connect(host=hostip, user=username, passwd=password, db=database)
cursor = db.cursor()
sql = dbQueryForRetrieveInvoiceCount;

try:
   cursor.execute(sql)
   results = cursor.fetchall()
   for row in results:
        syncDBInvoiceCount = row[0]
except:
    print dbError
    sys.exit(3)

#print(apiInvoiceCount)
#print(syncDBInvoiceCount)   
#invoiceDBList1 = ['1111','22222','3333','55555','77777','4444']
#invoiceAPIList1 = ['1111','656565','22222','56555','77777','4444','323232','3333']

if (apiInvoiceCount == syncDBInvoiceCount):
    if os.path.isfile(path+filename) and os.path.getsize(path+filename) > 0:
        f = open(path+filename,'r+')
        f.truncate(0)
        f.close()
        
    print okMessage
    sys.exit(0)

elif apiInvoiceCount <> syncDBInvoiceCount:
     if os.path.isfile(path+filename) and os.path.getsize(path+filename) > 0:
        f = open(path+filename,'r+')
        data = f.read()
        oldDate = datetime.datetime.strptime(data,dateformat)
        diff = (now-oldDate)
        if (diff >= checkedIn):
            try:
                cursor.execute(dbQueryForRetrieveInvoiceIdList)
                results = cursor.fetchall()
                for row in results:
                    invoiceDBList.append(row[0])
            except:
                print dbError
                sys.exit(3)
        
            for apiInvoiceId in invoiceAPIList:
                for dbInvoiceId in invoiceDBList:
                    if (apiInvoiceId == dbInvoiceId):
                        found = 1
                        invoiceDBList.remove(dbInvoiceId)
                        break;
                    else:
                        found = 0
      
                if (found == 1) :
                    connecterList.append(apiInvoiceId)
                    
            
            for Id in connecterList:
                for apiInvoiceId in invoiceAPIList:
                        if (Id == apiInvoiceId):
                            invoiceAPIList.remove(apiInvoiceId)

                   
            dbInvoicesMissedMessage = '/DiffDBLevel-InvoiceIds:' + ','.join(invoiceDBList)
            apiInvoicesMissedMessage = '/DiffAPILevel-invoiceIds:' + ','.join(invoiceAPIList)
            print (criticalMessage + dbInvoicesMissedMessage + apiInvoicesMissedMessage)
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


db.close()






