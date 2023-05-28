import pandas as pd
import os 
from geopy import distance
import re

#Arrow Steps:
#1. Use Properties map view on NIC website. Hover over green market bubbles and zoom in to export properties as batches. Overlap on export is fine.
#2. Download files to NIC > Data > Properties. 
#3. Run this script to create the search template. The script will remove properties that are outside of the subscription area, too close to the border, or flagged by NIC as preventing the analysis from running.

#Omni Steps:
#1. Run analysis using the Omni Major Market Centers file in the Data folder for 20 mile radius.
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
df = pd.read_csv(file_path, usecols=['FacilityName', 'Id', 'Name', 'Address', 'City', 'State', 'Zip', 'Latitude.1', 'Longitude.1', 'ALExistingBeds', 'ILExistingBeds', 'MCExistingBeds'])
df = df.rename(columns={    'Id': 'ID (Optional)',
                            'Name': 'Facility Name',
                            'Address': 'Street Address',
                            'Zip': 'Zip Code',
                            'Latitude.1': 'Lat (Optional)',
                            'Longitude.1': 'Long (Optional)'})

df = df.drop_duplicates(subset=['Facility Name', 'Street Address'])
df['Total Inventory'] = df['ALExistingBeds'] + df['ILExistingBeds'] + df['MCExistingBeds']
df = df.sort_values(by=['Total Inventory'], ascending=False)
df = df.drop(columns=['Total Inventory', 'ALExistingBeds', 'ILExistingBeds', 'MCExistingBeds'])
export_df = pd.DataFrame()

df['FacilityName'] = df['FacilityName'].str.split(',').str[1].str.strip()
df = df.drop_duplicates(subset=['FacilityName', 'Facility Name', 'Street Address'])

continue_export = True
pass_count = 1
while continue_export:
    print(f'Pass {pass_count}')
    pass_df = pd.DataFrame()
    drop_list = []
    distance_threshold = 10.5
    market_areas = df['FacilityName'].unique().tolist()
    for market in market_areas:
        print(f'Trimming communities in close proximity for {market}')
        market_df = df[df['FacilityName'] == market]
        for index, row in market_df.iterrows():
            site = row['ID (Optional)']
            if site in drop_list:
                continue
            else:
                site_lat = row['Lat (Optional)']
                site_lon = row['Long (Optional)']
                search_df = market_df.copy()
                if search_df.shape[0] > 1:
                    search_df = search_df[search_df['ID (Optional)'] != site]
                    search_df['Distance'] = search_df.apply(lambda x: distance.distance((x['Lat (Optional)'],x['Long (Optional)']),(site_lat, site_lon)).miles, axis=1)
                    drop_sites = search_df[search_df['Distance'] <= distance_threshold]['ID (Optional)'].unique().tolist()
                    drop_list += drop_sites

                    if len(drop_sites) > 0:
                        print(f'Found {len(drop_sites)} sites within {distance_threshold} mile of {site}')

                site_df = pd.DataFrame({    'ID (Optional)': [site], 
                                            'Facility Name': [row['Facility Name']], 
                                            'Street Address': [row['Street Address']],
                                            'City': [row['City']],
                                            'State': [row['State']],
                                            'Lat (Optional)': [site_lat], 
                                            'Long (Optional)': [site_lon]})

                export_df = pd.concat([export_df, site_df])
                pass_df = pd.concat([pass_df, site_df])

    print('Starting last check across import for sites within radius')
    drop_list = []
    for index, row in pass_df.iterrows():
        site = row['ID (Optional)']
        if site in drop_list:
            continue
        else:
            lat = row['Lat (Optional)']
            lon = row['Long (Optional)']
            pass_df['Distance'] = pass_df.apply(lambda x: distance.distance((x['Lat (Optional)'],x['Long (Optional)']),(lat, lon)).miles, axis=1)
            drop_sites = pass_df[(pass_df['Distance'] <= distance_threshold) & (pass_df['ID (Optional)'] != site)]['ID (Optional)'].unique().tolist()
            if len(drop_sites) > 0:
                print(f'Found {len(drop_sites)} sites within {distance_threshold} mile of {site}')
            drop_list += drop_sites

    pass_df = pass_df[~pass_df['ID (Optional)'].isin(drop_list)]
    export_df = export_df[~export_df['ID (Optional)'].isin(drop_list)]
    if len(drop_list) > 0:
        print(f'Dropped {len(drop_list)} sites within radius from import')

    file_path = os.path.join(os.getcwd(), 'Output', f'OmniSearchTemplate{pass_count}.xlsx')
    pass_df.to_excel(file_path, index=False)
    print(f'Pass {pass_count} Omni Search Template created successfully with {pass_df.shape[0]} sites.')

    df = df[~df['ID (Optional)'].isin(export_df['ID (Optional)'].tolist())]
    pass_count += 1
    if df.shape[0] == 0 or pass_count > 15:
        continue_export = False