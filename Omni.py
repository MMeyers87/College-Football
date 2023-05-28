import pandas as pd
import os
from geopy import distance

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

"""
drop_list = []
distance_threshold = 1
market_areas = df['FacilityName'].unique().tolist()
for market in market_areas:
    print(f'Trimming communities in close proximity for {market}')
    market_df = df[df['FacilityName'] == market]
    market_df = market_df.loc[:,['ID (Optional)', 'Lat (Optional)', 'Long (Optional)']]
    search_df = market_df.copy()
    for index, row in search_df.iterrows():
        search_id = row['ID (Optional)']
        if search_id in drop_list:
            continue
        else:
            search_lat = row['Lat (Optional)']
            search_lon = row['Long (Optional)']
            market_df['Distance'] = market_df.apply(lambda x: distance.distance((x['Lat (Optional)'],x['Long (Optional)']),(search_lat, search_lon)).miles, axis=1)
            drop_sites = market_df[(market_df['Distance'] <= distance_threshold) & (market_df['ID (Optional)'] != search_id)]['ID (Optional)'].unique().tolist()
            if len(drop_sites) > 0:
                print(f'Found {len(drop_sites)} sites within {distance_threshold} mile of {search_id}')
            drop_list += drop_sites
            market_df = market_df[(market_df['Distance'] > distance_threshold) | (market_df['ID (Optional)'] == search_id)]
            market_df = market_df.drop(columns=['Distance'])

df = df[~df['ID (Optional)'].isin(drop_list)]    
drop_cols = ['FacilityName', 'ALExistingBeds', 'ILExistingBeds', 'MCExistingBeds']
df = df.drop(columns=drop_cols)

rows = df.shape[0]
chunksize = 2000
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
print(f' {i-1} Omni Search Template(s) round 1 created successfully.')

#Round 2
folder_path = os.path.join(os.getcwd(), 'Data', 'Omni Round 1 For Comps')
file_list = os.listdir(folder_path)
df = pd.DataFrame()
for file in file_list:
    file_path = os.path.join(folder_path, file)
    temp_df = pd.read_excel(file_path, usecols=['StreetAddress', 'City', 'State', 'FacilityName', 'ZipCode', 'Latitude', 'Longitude', 'Number of Campus Rentals'])
    temp_df = temp_df[temp_df['Number of Campus Rentals'] >= 3]
    temp_df = temp_df.rename(columns={  'StreetAddress': 'ID (Optional)',
                                        'City': 'Facility Name',
                                        'State': 'Street Address',
                                        'FacilityName': 'State',
                                        'ZipCode': 'Zip Code',
                                        'Latitude': 'Lat (Optional)',
                                        'Longitude': 'Long (Optional)'})  
    df = pd.concat([df, temp_df])

df = df.drop_duplicates(subset=['ID (Optional)', 'Facility Name'])
df = df.drop(columns=['Number of Campus Rentals'])

rows = df.shape[0]
chunksize = 2000
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
"""