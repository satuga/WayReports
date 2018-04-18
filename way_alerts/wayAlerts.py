#
# price update Alerts
#

#imports
import sys
import string
import MySQLdb
import datetime
import time
import smtplib
import csv
import email
import email.mime.application
from email.MIMEMultipart import MIMEMultipart
from email.MIMEText import MIMEText
from email.MIMEImage import MIMEImage
from email import Encoders
from email.MIMEBase import MIMEBase
from email.Utils import formatdate
import mimetypes
import os
from fpdf import FPDF, HTMLMixin
import os.path
import json
import sendgrid
from sendgrid.helpers.mail import *
import base64
import sendgrid
import os
from sendgrid.helpers.mail import Email, Content, Mail, Attachment
try:
    # Python 3
    import urllib.request as urllib
except ImportError:
    # Python 2
    import urllib2 as urllib


ts = time.time()
st = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
filePath = "email_body"

FILENAME_PRICEUPDATE="priceUpdate.csv"
FILENAME_SOLDOUT="soldOutAlert.csv"
CFG_FILENAME="wayAlerts.cfg"
LRT_FILENAME="last_run_time"

PRICEUPDATE_REPORT_HISTORY=12
SOLDOUT_REPORT_HISTORY=12
INACTIVE_REPORT_HISTORY=2


INACTIVE_REPORT_QUERY="""
select p.lst_listingId parent, c.lst_listingid child, concat(p.lst_listingName, ' -  ', c.lst_listingName) Listing, c.lst_status Status, c.lst_lastModifiedDateTime DeactivationTime
  from tbl_listing c, tbl_listing p
 where c.lst_parentListingId = p.lst_ListingId
   and c.lst_status='in-active'
   and p.lst_status <> 'in-Active'
   and c.lst_ser_serviceID = 45
   and c.lst_lastModifiedDateTime >  convert_tz(date_sub("{0}", interval {1:d} hour), 'America/Los_Angeles', 'GMT')
union
select p.lst_listingId parent, c.lst_listingid child, p.lst_listingName Listing, p.lst_status Status, p.lst_lastModifiedDateTime DeactivationTime
  from tbl_listing c, tbl_listing p
 where c.lst_parentListingId = p.lst_ListingId
   and p.lst_status='in-active'
   and p.lst_ser_serviceID = 45
   and p.lst_lastModifiedDateTime >  convert_tz(date_sub("{0}", interval {1:d} hour), 'America/Los_Angeles', 'GMT')
group by 2
order by 1;
"""

PRICEUPDATE_SUMMARY_QUERY="""
 select nml_listingCode `IATA`,
        nml_listingName `Airport`,
        c.lst_listingID `ListingId`,
        convert(concat(p.lst_listingName, ' -  ', c.lst_listingName) using ascii) `Listing`
   from tbl_listing c
   join tbl_listing p on c.lst_parentListingId = p.lst_listingId
   join tbl_listing_price on lpr_lst_listingId = c.lst_listingId
   join tbl_listing_schedule on c.lst_listingID=LSH_LST_ListingID
   join tbl_listing_schedule_price on lsh_listingScheduleId=lsp_lsh_listingScheduleId
   join tbl_near_me on p.lst_listingId=nme_lst_listingID
   join tbl_near_me_listing on nme_nml_nearMelistingID=nml_nearMeListingID
  where LSP_LastModifiedDateTime > date_sub("{0}", interval {1:d} hour)
  group by ListingId
  order by IATA;
"""


PRICEUPDATE_REPORT_QUERY="""
 select nml_listingCode `IATA`,
        nml_listingName `Airport`,
        c.lst_listingID `ListingId`,
        concat(p.lst_listingName, ' -  ', c.lst_listingName) `Listing`,
        LSH_ScheduleStartDateTime `From`,
        LSH_ScheduleEndDateTime `To`,
        LPR_ListingPrice `Base Price`,
        lsp_listingPrice `New Price`,
        convert_tz(LSP_LastModifiedDateTime, 'GMT', 'America/Los_Angeles') `UpdateTime`
   from tbl_listing c
   join tbl_listing p on c.lst_parentListingId = p.lst_listingId
   join tbl_listing_price on lpr_lst_listingId = c.lst_listingId
   join tbl_listing_schedule on c.lst_listingID=LSH_LST_ListingID
   join tbl_listing_schedule_price on lsh_listingScheduleId=lsp_lsh_listingScheduleId
   join tbl_near_me on p.lst_listingId=nme_lst_listingID
   join tbl_near_me_listing on nme_nml_nearMelistingID=nml_nearMeListingID
  where LSP_LastModifiedDateTime > date_sub("{0}", interval {1:d} hour)
  order by IATA;
"""

SOLDOUT_SUMMARY_QUERY="""
select IATA,
       Airport,
       ParkingLocationListingID,
       ParkingTypeListingID,
       Listing,
       group_concat(DISTINCT soldOutDate ORDER by soldOutDate SEPARATOR ', ')
  from (
       select
              nml_listingCode `IATA`,
              nml_listingName `Airport`,
              LSTParent.LST_ListingID ParkingLocationListingID,
              LSH_LST_ListingID ParkingTypeListingID,
              convert(concat(LSTParent.LST_ListingName, ' -  ', LSTChild.LST_ListingName) using ascii) Listing,
              Capacity,
              case when LSP_ParamValue='Yes' then MinBooked else MaxBooked end  Booked,
              case when LSP_ParamValue='Yes' then MaxAvailable else MinAvailable end  Available,
              soldOutDate
         from (
              select
                     LSH_LST_ListingID,
                     LRA_Date soldOutDate,
                     max(LRA_Capacity) Capacity,
                     max(LRA_Booked) MaxBooked,
                     min(LRA_Booked) MinBooked,
                     max(LRA_Available) MaxAvailable,
                     min(LRA_Available) MinAvailable
                from tbl_listing_schedule LSH
              inner join tbl_listing_rolling_availability on LRA_LSH_ListingScheduleID=LSH_ListingScheduleID
              where
                    LRA_EndTime <> '23:59:00'
                and LRA_Date between date(now()) and date(date_add(now(), interval 10 day))
              group by LSH_LST_ListingID,LRA_Date
               ) ObjAvail
       inner join tbl_listing LSTChild on LSH_LST_ListingID = LSTChild.LST_ListingID
       inner join tbl_listing LSTParent on LSTParent.LST_ListingID = LSTChild.LST_ParentListingID
       inner join (select LSP_LST_ListingID,LSP_ParamValue
                       from tbl_listing_attrs
                      where  LSP_SEK_ServiceKeyID=38
                    ) OptStatus on LSP_LST_ListingID= LSH_LST_ListingID
             join tbl_near_me on LSTParent.lst_listingId=nme_lst_listingID
             join tbl_near_me_listing on nme_nml_nearMelistingID=nml_nearMeListingID
       where LSTParent.LST_Status='Active'
        -- and LSTParent.LST_ListingID=1581281
       order by ParkingLocationListingID
       ) RepTab
 where Capacity <> 1
   and IFNULL(Booked/(case when Capacity<Booked then Booked else Capacity end)*100,0) >=90
  group by ParkingLocationListingId
 order by IATA, ParkingLocationListingID, soldOutDate;
"""

SOLDOUT_REPORT_QUERY="""
select *,
       IFNULL(Booked/(case when Capacity<Booked then Booked else Capacity end)*100,0) Utilization
  from (
       select
              nml_listingCode `IATA`, 
              nml_listingName `Airport`,
              LSTParent.LST_ListingID ParkingLocationListingID,
              LSH_LST_ListingID ParkingTypeListingID,
              concat(LSTParent.LST_ListingName, ' -  ', LSTChild.LST_ListingName) Listing,
              Capacity,
              case when LSP_ParamValue='Yes' then MinBooked else MaxBooked end  Booked,
              case when LSP_ParamValue='Yes' then MaxAvailable else MinAvailable end  Available,
              soldOutDate
         from (
              select 
                     LSH_LST_ListingID,
                     LRA_Date soldOutDate,
                     max(LRA_Capacity) Capacity,
                     max(LRA_Booked) MaxBooked,
                     min(LRA_Booked) MinBooked,
                     max(LRA_Available) MaxAvailable,
                     min(LRA_Available) MinAvailable
                from tbl_listing_schedule LSH
              inner join tbl_listing_rolling_availability on LRA_LSH_ListingScheduleID=LSH_ListingScheduleID
              where
                    LRA_EndTime <> '23:59:00'
                and LRA_Date >= date(now())
              group by LSH_LST_ListingID,LRA_Date
               ) ObjAvail
       inner join tbl_listing LSTChild on LSH_LST_ListingID = LSTChild.LST_ListingID
       inner join tbl_listing LSTParent on LSTParent.LST_ListingID = LSTChild.LST_ParentListingID
       inner join (select LSP_LST_ListingID,LSP_ParamValue
                       from tbl_listing_attrs
                      where  LSP_SEK_ServiceKeyID=38
                    ) OptStatus on LSP_LST_ListingID= LSH_LST_ListingID
             join tbl_near_me on LSTParent.lst_listingId=nme_lst_listingID
             join tbl_near_me_listing on nme_nml_nearMelistingID=nml_nearMeListingID
       where LSTParent.LST_Status='Active'
        -- and LSTParent.LST_ListingID=1581281
       order by ParkingLocationListingID
       ) RepTab
 where Capacity <> 1
   and IFNULL(Booked/(case when Capacity<Booked then Booked else Capacity end)*100,0) >=90
 order by IATA, ParkingLocationListingID, soldOutDate;
"""

PRICEUPDATE_REPORT_HDR= "Iata, Airport, ListingID,Listing,From,To,Base Price,Price,Updated At"
SOLDOUT_REPORT_HDR= "IATA,Airport,ParentId,ChildId,Listing,Capacity,Booked,Available,soldOutDate,Utilization"

EMAIL_BODY_HEADER = """ <html>
<head>
<style>
table {
    font-family: arial, sans-serif;
    border-collapse: collapse;
    width: 100%;
}

td, th {
    border: 1px solid #dddddd;
    text-align: left;
    padding: 8px;
}

tr:nth-child(even) {
    background-color: #dddddd;
}
</style>
</head>
<body>
<H1>Way Operational Alerts - Parking</H1>
<br>
Please see attached csv files for Alert details.
<br>
<br>
"""

INACTIVE_REPORT_COLUMNS=['ParentId', 'ChildId', 'Listing', 'Status', 'Last Modified Time']
INACTIVE_REPORT_TITLE="Deactivated Lots Report"
INACTIVE_REPORT_DEFAULT = """
   <tr>
    <td colspan="{0:d}" align="center">There are no <b>deactivated</b> lots since {1}</td>
  </tr>
 """
INACTIVE_REPORT_FOOTER = " </table> "

PRICE_REPORT_COLUMNS=['IATA', 'Airport', 'ListingId', 'Listing']
PRICE_REPORT_TITLE="Pricing Updates Summary"
PRICE_REPORT_DEFAULT = """
   <tr>
    <td colspan="{0:d}" align="center">There are no <b>Price Updates</b> since {1}</td>
  </tr>
 """
PRICE_REPORT_FOOTER = " </table> "

SOLDOUT_REPORT_COLUMNS=['IATA', 'Airport', 'ParentId', 'ChildId', 'Listing', 'Date']
SOLDOUT_REPORT_TITLE="Soldout Lots 10 day Summary"
SOLDOUT_REPORT_DEFAULT = """
   <tr>
    <td colspan="{0:d}" align="center">There are no <b>soldout</b> lots since {1}</td>
  </tr>
 """
SOLDOUT_REPORT_FOOTER = " </table> "


EMAIL_BODY_FOOTER="""

</body>
</html>"""




SENDGRID_KEY="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"
# SENDGRID_KEY="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"
# SENDGRID_KEY="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"
# 
#
def emailGrid(email_listing, email_body):

	sg = sendgrid.SendGridAPIClient(apikey = SENDGRID_KEY)
	from_email = Email("support@way.com", "Support")
	subject = "ALERT: Price Update + Sold Out Report "
	to_email = Email("WayEmployees@way.com","Recipients")
	#content = Content("text/html", "Price Update + Sold Out Alert<br/><br/>Price updates in the last {0:d} hours. <br/><br/>Please See Attached.".format(PRICEUPDATE_RPT_HISTORY))
	content = Content("text/html", email_body)

	file_path = FILENAME_PRICEUPDATE
	with open(file_path,'rb') as f:
	    data = f.read()
	    f.close()
	encoded = base64.b64encode(data).decode()

	path_of_attachment = FILENAME_SOLDOUT
	with open(path_of_attachment,'rb') as f2:
	    data2 = f2.read()
	    f2.close()
	encoded2 = base64.b64encode(data2).decode()

	priceAttachment = Attachment()
	priceAttachment.content = encoded
	priceAttachment.type = "application/csv"
	priceAttachment.filename = FILENAME_PRICEUPDATE
	priceAttachment.disposition = "attachment"
	priceAttachment.content_id = "Price Update Reports"

	soldOutAttachment = Attachment()
	soldOutAttachment.content = encoded2
	soldOutAttachment.type = "application/csv"
	soldOutAttachment.filename = FILENAME_SOLDOUT
	soldOutAttachment.disposition = "attachment"
	soldOutAttachment.content_id = "Sold Out Reports"

	mail = Mail(from_email, subject, to_email, content)

	personalization = Personalization()

	##List of Recipients
	for i in range (0,len(email_listing)):
		personalization.add_to(Email(email_listing[i]))

	mail.add_personalization(personalization)

	mail.add_attachment(priceAttachment)
	mail.add_attachment(soldOutAttachment)

	try:
	    response = sg.client.mail.send.post(request_body=mail.get())
	except urllib.HTTPError as e:
	    print(e.read())
	    exit()
#



#
def generateReport( conn, query, last_run_time, report_history ):

        reportRows = []

        cur = conn.cursor()

        query = query.format(last_run_time, report_history)
        #print query
        #print
	#print
        #sys.exit(1)

        try:
                cur.execute(query)
                reportRows = cur.fetchall()
        except MySQLdb.Error, e:
                print "Error %d: %s" % (e.args[0], e.args[1])
        return reportRows

#


#
def generateInactiveReport( conn, last_run_time ):

	reportRows = []

	cur = conn.cursor()

	query = """select p.lst_listingId parent, c.lst_listingid child, concat(p.lst_listingName, ' -  ', c.lst_listingName) Listing, c.lst_status Status, c.lst_lastModifiedDateTime DeactivationTime
  from tbl_listing c, tbl_listing p
 where c.lst_parentListingId = p.lst_ListingId
   and c.lst_status='in-active'
   and p.lst_status <> 'in-Active'
   and c.lst_ser_serviceID = 45
   and c.lst_lastModifiedDateTime >  convert_tz(date_sub("{0}", interval {1:d} hour), 'America/Los_Angeles', 'GMT') 
union
select p.lst_listingId parent, c.lst_listingid child, p.lst_listingName Listing, p.lst_status Status, p.lst_lastModifiedDateTime DeactivationTime
  from tbl_listing c, tbl_listing p
 where c.lst_parentListingId = p.lst_ListingId
   and p.lst_status='in-active'
   and p.lst_ser_serviceID = 45
   and p.lst_lastModifiedDateTime >  convert_tz(date_sub("{0}", interval {1:d} hour), 'America/Los_Angeles', 'GMT')
group by 2
order by 1;"""

        query = query.format(last_run_time, INACTIVE_REPORT_HISTORY)
        #print query
        #sys.exit(1)

 	try:
		cur.execute(query)
		reportRows = cur.fetchall()
	except MySQLdb.Error, e:
		print "Error %d: %s" % (e.args[0], e.args[1])
	return reportRows
#

def createCSV(fileName, report_from_time, report_to_time, report_header, report):

	f = open(fileName, 'w')
	title = "DATE:,%s to %s\n" % (report_from_time, report_to_time)
	f.write(title)
	f.write(report_header + "\n")

        for row in report:
		l = ""
		for field in row:
			if len(l) > 0:
				l += ", "
			l += str(field)
		l += "\n"
		f.write(l)
	f.close()
#


def generateEmailBody(last_run_time, report, columns, title, defaultRow, footer):

	# generate report header
	#
	htmlHeader = "<br> <h2> " + title + " </h2> "
	htmlHeader += "<table> <tr>\n "

	for field in columns:
		htmlHeader += "<th>{}</th> ".format(field)
	htmlHeader += " </tr>\n "	
        
	# generate report body
	#	
	reportRows=''
        if not report:
		reportRows = defaultRow.format(len(columns), last_run_time)
        
	for row in report:
                r = ''
		for field in row:
			r += "<td>{}</td> ".format(field)
		reportRows += "<tr> " + r + "</tr> \n "

	# generate footer
	#
        htmlFooter = footer

	htmlReport = htmlHeader + reportRows + htmlFooter

	return htmlReport


### UTLITY FUNCTIONS #####
#
def read_file(f):
	with open(f, "r") as reader:
		return reader.readline()
#

#
def write_file(filename, contents):
	f = open(filename, 'w')
	f.write(contents)
	f.close()
#

#
def init_Setup():

	configs = []
	db_info = []
	email_listing = []

	file = open(CFG_FILENAME, "r") 
	for line in file:
		line = line.replace("\n","")
		line = line.replace(" ","")
		configs.append(str(line))

	begin = configs.index("<database_information>") + 1
	end =  configs.index("</database_information>")


	while begin != end:
		db_info.append(configs[begin])
		begin += 1


	begin = configs.index("<email_listing>") + 1
	end =  configs.index("</email_listing>")

	while begin != end:
		email_listing.append(configs[begin])
		begin += 1

	return db_info, email_listing

#
def main():

	email_body = ""

	db_info = []
	email_listing = []
        priceReport = []
        soldOutReport = []
        inactiveReport = []

	(db_info, email_listing) = init_Setup()

	# get last run time from LRT_FILENAME 
	if(os.path.exists(LRT_FILENAME)):
		last_time = read_file(LRT_FILENAME)
	else:
		last_time = st;

	try:
		myConnection = MySQLdb.connect(host=db_info[0], user=db_info[1], passwd=db_info[2], db=db_info[3])

		priceSummary = generateReport(myConnection, PRICEUPDATE_SUMMARY_QUERY, last_time, PRICEUPDATE_REPORT_HISTORY)
		priceReport  = generateReport(myConnection, PRICEUPDATE_REPORT_QUERY, last_time, PRICEUPDATE_REPORT_HISTORY)

		soldOutSummary = generateReport(myConnection, SOLDOUT_SUMMARY_QUERY, last_time, SOLDOUT_REPORT_HISTORY)
		soldOutReport  = generateReport(myConnection, SOLDOUT_REPORT_QUERY, last_time, SOLDOUT_REPORT_HISTORY)

		inactiveReport = generateReport(myConnection, INACTIVE_REPORT_QUERY, last_time, INACTIVE_REPORT_HISTORY)

		myConnection.close()
	except MySQLdb.Error, e:
		print "Error %d: %s" % (e.args[0], e.args[1])
		raise Exception("DB Exception: {}: {}".format(e.args[0], e.args[1]))


	#update time file for last run
	write_file(LRT_FILENAME, st)

	#create csv files to record pulled content
	createCSV(FILENAME_PRICEUPDATE, last_time, st, PRICEUPDATE_REPORT_HDR, priceReport)

	createCSV(FILENAME_SOLDOUT, last_time, st, SOLDOUT_REPORT_HDR, soldOutReport)

	email_body = generateEmailBody(last_time, inactiveReport, INACTIVE_REPORT_COLUMNS, INACTIVE_REPORT_TITLE, INACTIVE_REPORT_DEFAULT, INACTIVE_REPORT_FOOTER)
	email_body += generateEmailBody(last_time, priceSummary, PRICE_REPORT_COLUMNS, PRICE_REPORT_TITLE, PRICE_REPORT_DEFAULT, PRICE_REPORT_FOOTER)
	email_body += generateEmailBody(last_time, soldOutSummary, SOLDOUT_REPORT_COLUMNS, SOLDOUT_REPORT_TITLE, SOLDOUT_REPORT_DEFAULT, SOLDOUT_REPORT_FOOTER)

	email_body = EMAIL_BODY_HEADER + email_body + EMAIL_BODY_FOOTER

        #printable = set(string.printable)
        #filter(lambda x: x in printable, email_body)

        #print email_body
        #sys.exit(1)

	emailGrid(email_listing, email_body)
#

##Run Main##
if __name__ == "__main__": 
    main()
