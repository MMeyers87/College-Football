from dotenv import load_dotenv
import requests
import pandas as pd
import os

load_dotenv()
token = os.getenv('CFB_API_TOKEN')
auth_header = {'Authorization':f'Bearer {token}'}

def get_cfb_data(endpoint, params=None):
    url = f'https://api.collegefootballdata.com/{endpoint}'
    print(f'Requesting data from {url}')
    response = requests.get(url, headers=auth_header, params=params)
    result = pd.DataFrame(response.json())
    print(f'Returned {result.shape[0]} rows.')
    return result

def get_teams_logos():
    teams = get_cfb_data('teams')
    venues = pd.json_normalize(teams['location'])
    teams = teams.join(venues).drop(columns=['location'])
    logos = teams.loc[:,['id','logos']].explode('logos')
    teams = teams.drop(columns=['logos'])
    return teams, logos

def get_player_game_stats(season, week):
    player_stats = get_cfb_data('games/players', params={'year':season, 'week':week})
    if player_stats.shape[0] > 0:
        game_df = player_stats.explode('teams')
        school_df = pd.json_normalize(game_df['teams'])
        game_df = game_df.drop(columns=['teams']).reset_index(drop=True)
        game_df = game_df.rename(columns={'id':'game_id'})
        game_df['season'] = season
        game_df['week'] = week
        school_df = game_df.join(school_df)
        category_df = school_df.explode('categories')
        category_df = category_df.drop(columns=['points'])
        stat_type_df = pd.json_normalize(category_df['categories'])
        stat_type_df = category_df.drop(columns=['categories']).join(stat_type_df)
        stat_type_df = stat_type_df.explode('types')
        stat_detail_df = pd.json_normalize(stat_type_df['types'])
        stat_type_df = stat_type_df.drop(columns=['types']).reset_index(drop=True)
        stat_type_df = stat_type_df.rename(columns={'name':'stat_category'})
        stat_detail_df = stat_type_df.join(stat_detail_df)
        stat_detail_df = stat_detail_df.explode('athletes')
        stat_detail_df = stat_detail_df.rename(columns={'name':'stat_type'})
        athlete_df = pd.json_normalize(stat_detail_df['athletes'])
        stat_detail_df = stat_detail_df.drop(columns=['athletes']).reset_index(drop=True)
        athlete_df = stat_detail_df.join(athlete_df)
        athlete_df = athlete_df.rename(columns={'name':'player_name', 'id':'player_id','stat':'stat_value'})
        athlete_df= athlete_df[athlete_df['stat_value'] != '0']
        athlete_df = athlete_df.drop_duplicates()
    else:
        athlete_df = pd.DataFrame({'season':[season],'week':[week]})
        print(f'No player stats for {season} week {week}')
    return athlete_df


