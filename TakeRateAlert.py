from __future__ import with_statement
from AnalyticsClient import AnalyticsClient
import pandas as pd
from bs4 import BeautifulSoup
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import datetime 

class Config:

    CLIENTID = "1000.DQ32DWGNGDO7CV0V1S1CB3QFRAI72K";
    CLIENTSECRET = "92dfbbbe8c2743295e9331286d90da900375b2b66c";
    REFRESHTOKEN = "1000.0cd324af15278b51d3fc85ed80ca5c04.7f4492eb09c6ae494a728cd9213b53ce";

    ORGID = "60006357703";
    VIEWID="174857000084814013";
    WORKSPACEID = "174857000004732522";
    
class sample:
    ac = AnalyticsClient(Config.CLIENTID, Config.CLIENTSECRET, Config.REFRESHTOKEN)

    def export_data(self, ac, view_ids):
        response_format = "csv"
        file_path_template = "TakeRate_{}.csv"
        bulk = ac.get_bulk_instance(Config.ORGID, Config.WORKSPACEID)
        for view_id in view_ids:
            file_path = file_path_template.format(view_id)
            bulk.export_data(view_id, response_format, file_path)

try:
    obj = sample()
    view_ids = ["174857000084814013"]  # Replace with your list of view IDs
    obj.export_data(obj.ac, view_ids)

except Exception as e:
    print(str(e))
    
    
    # 174857000084814013  :- Lower Take Rate View Id

df=pd.read_csv('TakeRate_174857000084814013.csv')
df=df.fillna(0)


# Final Mail Scheduling

current_date=datetime.datetime.now()
year=-current_date.year
month=current_date.strftime('%b')
day=current_date.strftime('%A')


# Email configuration
sender_email = 'akhil.anand@bijnis.com'
receiver_emails=['akhil.anand@bijnis.com','himanshu.jaiswal@bijnis.com']
subject = f'PP Low Margin Variants'

# Create the email messagefrom bs4 import BeautifulSoup

def apply_table_styles(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    table = soup.find('table')
    table['style'] = 'border-collapse: collapse; font-size: 14px;'
    for th in table.findAll('th'):
        th['style'] = 'border: 1px solid black; padding: 3px;'
    for td in table.findAll('td'):
        td['style'] = 'border: 1px solid black; padding: 3px;'
    return str(soup)


html_content1 = '<p><b>Lots Marked Ready but not Handed Over Ageing<b></p>'+ df.to_html(index=False, justify='center', classes='email-table')
html_content1 = apply_table_styles(html_content1)

message = MIMEMultipart()
message['From'] = sender_email
message['To'] = ', '.join(receiver_emails)
message['Subject'] = subject
message_text1 = 'Hi Everyone'
message_text2='Please Find below the list of variants X Size , with bijnis Take Rate less than 17 %'

# Attach HTML content to the email
## message.attach(MIMEText(str(html_content1) + '<br>' + str(html_content2) + str(html_content3)+str(html_content4) + str(html_content5), 'html'))
message.attach(MIMEText(message_text1+'<br>'+message_text2+'<br>'+str(html_content1),'html'))

# Connect to the SMTP server and send the email
if not df.empty:
    with smtplib.SMTP('smtp.gmail.com', 587) as server:
        server.starttls()
        server.login('akhil.anand@bijnis.com', 'bijnis@2')  # Replace 'your_password' with your actual password or use environment variables
        server.sendmail(sender_email, receiver_emails, message.as_string())
        print('Email sent successfully!')
else :
    print('No Data Avialable Skipping email sending')