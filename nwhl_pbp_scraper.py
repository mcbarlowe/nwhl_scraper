'''
this is a script designed to scrape the NWHL play by play JSON when given a 
game_id from the NWHL game database. I will reference play by play by the 
initials 'pbp' and dataframe as 'df' throughout the rest of these comments
'''
import requests
import pandas as pd 
import bs4


def get_pbp_dict(game_id):
    '''
    This function pulls the pbp json of the game id provided
    and returns the it as a python dictionary

    Inputs:
    game_id - unique number that represents a single NWHL game

    Ouputs:
    pbp_dict - python dictionary containing the json of the pbp for
               given game_id
    '''

    #pulling the json
    url = f'https://www.nwhl.zone/game/get_play_by_plays?id={game_id}'
    req = requests.get(url)

    #converting the json into a dictionary
    pbp_dict = req.json()

    return pbp_dict

def convert_pbp_dict(pbp_dict):
    '''
    this function converts the pbp_dict into three dataframes one for the 
    pbp, one for players, and one for the teams

    Inputs:
    pbp_dict - dictionary of the pbp json

    Outputs:
    pbp_df - dataframe of all play by play events
    player_df - dataframe of all players in the game
    team_df - dataframe of all teams in the game
    '''

    #getting the player and team dataframes this is pretty straightfoward as they
    #are just one level dictionaries inside the pbp JSON
    player_df = pd.DataFrame.from_dict(pbp_dict['roster_player'])
    team_df = pd.DataFrame.from_dict(pbp_dict['team_instance'])
    #pull out the pbp events
    pbp_list = pbp_dict['plays']

    #create list to hold each pbp event that I will then feed to pandas
    parsed_plays_list = []

    #loop through each event in the plays list and pull out the wanted features from
    #the dictionaries of each event
    for play in pbp_list:
        event_row = []
        event_row.append(play['play_index'])
        event_row.append(play['clock_time_string'])
        if play['special_tags'] and play['special_tags'][0] == "ends_time_interval":
            event_row.append(1200)
        else:
            time = play['clock_time_string'].split(':')
            minutes = int(time[0]) * 60
            seconds = int(time[1])
            event_row.append(minutes+seconds)
        event_row.append(play['game_id'])
        if play['special_tags'] and play['special_tags'][0] == "ends_time_interval":
            event_row.append('Period End')
        else:
            event_row.append(play['play_type'])
        event_row.append(play['play_by_play_string'])
        event_row.append(play['time_interval'])
        if "Goal" in play['play_by_play_string']:
            event_row.append(play['play_summary'].get('scorer_id'))
            event_row.append(play['play_summary'].get('assist_1_id'))
            event_row.append(play['play_summary'].get('assist_2_id'))
        else:
            event_row.append(play['primary_player_id'])
            event_row.append(play['play_summary'].get('loser_id'))
            event_row.append(play['play_summary'].get('assist_2_id'))
        event_row.append(play['team_id'])
        event_row.append(play['play_summary'].get('x_coord'))
        event_row.append(play['play_summary'].get('y_coord'))
        event_row.append(play['play_actions'][0].get('away_team_goalie'))
        event_row.append(play['play_actions'][0].get('home_team_goalie'))
        event_row.append(play['play_actions'][0].get('away_team_score'))
        event_row.append(play['play_actions'][0].get('home_team_score'))
        parsed_plays_list.append(event_row)

    #create column names for my new pbp_df
    cols = ['event_index', 'time', 'seconds_elapsed', 'game_id', 'event', 'event_description', 'period',
            'event_p1', 'event_p2', 'event_p3', 'event_team', 'x_coord', 'y_coord', 
            'away_goalie', 'home_goalie', 'away_score', 'home_score']

    #create df from list of list of each pbp event
    pbp_df = pd.DataFrame(parsed_plays_list, columns=cols)

    #fills na with zeros so I can cast player ids as 
    pbp_df = pbp_df.fillna(0)

    #pull in home and away team and team id 
    pbp_df['home_team'] = team_df.loc[0, 'name']
    pbp_df['home_team_id'] = team_df.loc[0, 'team_id']

    pbp_df['away_team'] = team_df.loc[1, 'name']
    pbp_df['away_team_id'] = team_df.loc[1, 'team_id']

#cast these keys as float's so I can join with the player_df to pull player names
    pbp_df['event_p1'] = pd.to_numeric(pbp_df['event_p1'], errors='coerce')
    pbp_df['event_p2'] = pbp_df['event_p2'].astype(float)
    pbp_df['event_p3'] = pbp_df['event_p3'].astype(float)
    pbp_df['away_goalie'] = pbp_df['away_goalie'].astype(float)
    pbp_df['home_goalie'] = pbp_df['home_goalie'].astype(float)

    pbp_df = pbp_df.sort_values(by = ['period', 'seconds_elapsed'])

    return pbp_df, player_df, team_df

def pull_player_names(pbp_df, player_df, id_column):
    '''
    this function pulls the player name of the column passed to it
    
    Inputs:
    pbp_df - play by play dataframe
    player_df - player dataframe with player ids
    id_column - pbp_df id column for which you want names matched to

    Outputs:
    pbp_df - play by play dataframe but with names for the id_column 
             passed to it
    '''

    #merge to the two dataframes to create a name dataframe which will be joined
    #back to the pbp_df 
    player_df['full_name'] = player_df['first_name'] + ' ' + player_df['last_name']
    
    pbp_df = pbp_df.merge(player_df[['id', 'full_name']], how='left', left_on=id_column, right_on='id')

    pbp_df = pbp_df.rename(columns = {'full_name': f'{id_column}_name'})

    return pbp_df

def main():
    '''
    This will run and return text files of the team_df, player_df and pbp_df as 
    pipe delimited files
    '''

    game_id = input("Please input game id you want scraped: ") 
    #game_id = 18507472
    pbp_dict = get_pbp_dict(game_id)
    pbp_df, player_df, team_df = convert_pbp_dict(pbp_dict)

    player_id_columns = ['event_p1', 'event_p2', 'event_p3', 'away_goalie', 'home_goalie']
    
    for column in player_id_columns:
        pbp_df = pull_player_names(pbp_df, player_df, column)
        
    pbp_df = pbp_df[['event_index', 'time', 'seconds_elapsed', 'game_id', 'event',
                     'event_description', 'period', 'event_p1', 'event_p1_name', 
                     'event_p2', 'event_p2_name', 'event_p3', 'event_p3_name', 'event_team',
                     'x_coord', 'y_coord', 'away_goalie', 'away_goalie_name', 'home_goalie',
                     'home_goalie_name', 'away_score', 'home_score', 'home_team_id', 'home_team',
                     'away_team_id', 'away_team']]
    pd.DataFrame.to_csv(pbp_df, 'pbp_df.txt', sep='|')
    pd.DataFrame.to_csv(player_df, 'player_df.txt', sep='|')
    pd.DataFrame.to_csv(team_df, 'team_df.txt', sep='|')

if __name__ == '__main__':
    main()
