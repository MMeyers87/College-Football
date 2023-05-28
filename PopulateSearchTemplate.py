import pandas as pd
import os 
from geopy import distance
import re

#Arrow Steps:
#1. Use Properties map view on NIC website. Hover over green market bubbles and zoom in to export properties as batches. Overlap on export is fine.
#2. Download files to NIC > Data > Properties. 
#3. Run this script to create the search template. The script will remove properties that are outside of the subscription area, too close to the border, or flagged by NIC as preventing the analysis from running.

#Omni Steps:
#1. Run analysis using the Omni Major Market Centers file in the Data folder for 50 mile radius.
#2. Run comp report in NIC, this will provide the list of individual properties to search for each market. Download the file to the Data folder as Omni Major Market Comps.csv
#3. Run this script to create the search templates for each market. The script will create multiple templates if the number of properties exceeds 5000.

#Arrow Properties
print('Preparing Arrow search template')
folder_path = os.path.join(os.getcwd(), 'Data', 'Properties')
file_list = os.listdir(folder_path)
print(f'Found {len(file_list)} files in {folder_path}')
df = pd.DataFrame()

for file in file_list:
    file_path = os.path.join(folder_path, file)
    file_df = pd.read_excel(file_path, sheet_name='Property Inventory', header=1, usecols=['Property ID', 'Property Name', 'Property Address', 'City', 'State', 'Zip Code', 'Latitude', 'Longitude'])
    df = pd.concat([df, file_df], ignore_index=True)
print('Files loaded successfully.')
print(f'Properties to search: {df.shape[0]} before exclusions')

#Remove states outside of subscription that were included in market area properties
exclude_states = ['GA', 'MS', 'KY', 'WI', 'AL']
exclude_df = df[df['State'].isin(exclude_states)].loc[:,['Latitude','Longitude']]
print(f'Found {len(exclude_df)} properties in excluded states.')
exclude_df = exclude_df.rename(columns={'Latitude': 'exclude lat', 'Longitude': 'exclude lon'})
df = df[~df['State'].isin(exclude_states)]
print(f'Properties to search: {df.shape[0]} after excluding states outside of subscription')

#Use excluded properties to filter out properties inside subscription states but within 5 miles of excluded states
edge_states = ['TN', 'FL', 'IL']
edge_df = df[df['State'].isin(edge_states)]

exclude_df['merge_on'] = 1
edge_df['merge_on'] = 1
edge_df = edge_df.merge(exclude_df, on='merge_on')
edge_df = edge_df.drop(columns=['merge_on'])

edge_df['Distance to Exclude'] = edge_df.apply(lambda x: distance.distance((x['Latitude'],x['Longitude']),(x['exclude lat'], x['exclude lon'])).miles, axis=1)
edge_df = edge_df[edge_df['Distance to Exclude'] <= 7]
exclude_properties = edge_df['Property ID'].unique().tolist()
print(f'Found {len(exclude_properties)} properties within 7 miles of excluded states.')

df = df[~df['Property ID'].isin(exclude_properties)]
print(f'Properties to search: {df.shape[0]} after excluding sites too close to border')

#Remove properties that NIC flags as preventing the analysis from running. Add additional elements to end of string. Can freeze html in order to extract using the following in console: setTimeout(function(){debugger;}, 5000)
error_html = 'inserted" style="">Site Mercy Harvard Care Center with buffer 5 miles is outside of eligible market areas.</span>'
buffer_matches = re.findall(r"Site\s+([A-Za-z\s]+)with", error_html)
market_matches = re.findall(r"Site\s+([A-Za-z\s]+)was", error_html)
manual_matches = [ 'Mercy Harvard Care Center', 'Cedarhurst of Kenosha', 'Brookdale Kenosha', 'The Langford of Collierville', 'Germantown Plantation Senior Living', 'Heritage at Irene Woods', 'Gardens of Germantown', 'Quince Nursing and Rehabilitation Center', 'The Jane Adams House', 'Dayspring Senior Living LLC', 'Quality Health of Fernandina', 'Ashwood Square', 'Hickory Valley Retirement Center', 'Morning Pointe of Collegedale at Greenbriar Cove', 'American House Shallowford', 'Highview in the Woodlands', 'Savannah Grand of Amelia Island', 'Creekside at Shallowford', 'Viviant Health Care of Chattanooga', 'Standifer Place', 'Southern Oaks', 'Martin Boyd Christian Home', 'StoryPoint Collierville', 'The Farms at Bailey Station', 'Beech Tree Manor', 'Morning Pointe of East Hamilton', 'The Bridge at South Pittsburg', 'Garden Plaza of Greenbriar Cove', 'Mercy Harvard Care Center', 'Cedarhurst of Kenosha', 'Brookdale Kenosha', 'The Langford of Collierville', 'Germantown Plantation Senior Living', 'Heritage at Irene Woods', 'Gardens of Germantown', 'Quince Nursing and Rehabilitation Center', 'The Jane Adams House', 'Dayspring Senior Living LLC', 'Quality Health of Fernandina', 'Ashwood Square', 'Hickory Valley Retirement Center', 'Morning Pointe of Collegedale at Greenbriar Cove', 'American House Shallowford', 'Highview in the Woodlands', 'Savannah Grand of Amelia Island', 'Creekside at Shallowford', 'Viviant Health Care of Chattanooga', 'Standifer Place', 'Southern Oaks', 'Martin Boyd Christian Home', 'StoryPoint Collierville', 'The Farms at Bailey Station', 'Beech Tree Manor', 'Morning Pointe of East Hamilton', 'The Bridge at South Pittsburg', 'Garden Plaza of Greenbriar Cove', 'Morning Pointe Senior Living, Chattanooga', 'Schilling Gardens & the Arbors at Schilling Gardens', 'Collerville Nursing & Rehab']
remove_matches = manual_matches + buffer_matches + market_matches
remove_matches = [x.strip() for x in remove_matches]

df = df[~df['Property Name'].isin(remove_matches)]
print(f'Removed {len(remove_matches)} properties that prevent the analysis from running.')
print(f'Properties to search: {df.shape[0]} after excluding NIC flagged properties')

#Format and export
df = df.drop_duplicates(subset=['Property Name', 'Property Address'])
df = df.rename(columns={    'Property ID': 'ID (Optional)',
                            'Property Name': 'Facility Name',
                            'Property Address': 'Street Address',
                            'Zip Code': 'Zip',
                            'Latitude': 'Lat (Optional)',
                            'Longitude': 'Long (Optional)'})

file_path = os.path.join(os.getcwd(), 'Output', 'ArrowSearchTemplate.xlsx')
df.to_excel(file_path, index=False)
print('Arrow search template created successfully.')

#Omni Properties
print('Preparing Omni search template')
file_path = os.path.join(os.getcwd(), 'Data', 'Omni Major Market Comps.csv')
df = pd.read_csv(file_path, usecols=['Id', 'Name', 'Address', 'City', 'State', 'Zip', 'Latitude', 'Longitude'])
df = df.rename(columns={    'Id': 'ID (Optional)',
                            'Name': 'Facility Name',
                            'Address': 'Street Address',
                            'Zip': 'Zip Code',
                            'Latitude': 'Lat (Optional)',
                            'Longitude': 'Long (Optional)'})

df = df.drop_duplicates(subset=['Facility Name', 'Street Address'])
rows = df.shape[0]
chunksize = 2500
more_chunks = True
i = 1
while more_chunks:
    chunk_df = df.iloc[:chunksize]
    print(chunk_df.shape[0])
    df = df.iloc[chunksize:]
    rows = df.shape[0]
    if rows == 0:
        more_chunks = False
    file_path = os.path.join(os.getcwd(), 'Output', f'OmniSearchTemplate{i}.xlsx')
    chunk_df.to_excel(file_path, index=False)
    i += 1
print(f' {i-1} Omni Search Template(s) created successfully.')
