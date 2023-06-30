import gspread
from oauth2client.service_account import ServiceAccountCredentials
import sys
import csv
# Set the path to your client secrets JSON file
credentials = ServiceAccountCredentials.from_json_keyfile_name( '/opt/airflow/config/client_secret.json',
                                                               ['https://spreadsheets.google.com/feeds',
                                                                'https://www.googleapis.com/auth/drive'])

sheetname = sys.argv[1]
worksheetname = sys.argv[2]
outputname = sys.argv[3]
# Authenticate using the credentials
client = gspread.authorize(credentials)

# Open the Google Sheet by its title
# sheet = client.open('IBM Dragon Boat')

sheet = client.open(sheetname)
# Select the worksheet where you want to insert the table

# worksheet = sheet.worksheet('Attendance')
worksheet = sheet.worksheet(worksheetname)
# Define your table data as a list of lists


# Set the values in the selected worksheet starting from cell A1
# worksheet.update('A1', table_data)
values = worksheet.get_all_values()

# Specify the path to save the CSV file
csv_file_path = '/opt/airflow/scripts/data/'+ outputname

# Save values to a CSV file
with open(csv_file_path, 'w', newline='') as csv_file:
    writer = csv.writer(csv_file)
    writer.writerows(values)
print('Table download successfully.')
