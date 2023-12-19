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

csv_file_path = '/opt/airflow/scripts/data/'+outputname
with open(csv_file_path, 'r') as file:
    csv_data = csv.reader(file)
    
    data = list(csv_data)
    if len(data) <= 1:
        print("Error: File export incorrectly!")
        sys.exit(1)

# Clear existing data in the worksheet
worksheet.clear()

# Append the CSV data to the worksheet
worksheet.append_rows(data)

print('CSV file imported successfully.')