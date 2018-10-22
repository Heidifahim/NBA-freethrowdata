import os
import pandas as pd

#Team Abbreviations - Team Name <key, value> dict
teamNameAbbr = dict(ATL='Atlanta Hawks', BKN='Brooklyn Nets', BOS='Boston Celtics', CHA='Charlotte Hornets', CHI='Chicago Bulls',
                    CLE='Cleveland Cavaliers', DAL='Dallas Mavericks', DEN='Denver Nuggets', DET='Detroit Pistons', GSW='Golden State Warriors',
                    HOU='Houston Rockets', IND='Indiana Pacers', LAC='Los Angeles Clippers', LAL='Los Angeles Lakers', MEM='Memphis Grizzlies',
                    MIA='Miami Heat', MIL='Milwaukee Bucks', MIN='Minnesota Timberwolves', NOP='New Orleans Pelicans', NYK='New York Knicks',
                    OKC='Oklahoma City Thunder', ORL='Orlando Magic', PHI='Philadelphia 76ers', PHX='Phoenix Suns', POR='Portland Trail Blazers',
                    SAC='Sacramento Kings', SAS='San Antonio Spurs', TOR='Toronto Raptors', UTA='Utah Jazz', WAS='Washington Wizards')

def version1_freethrows(df_freeThrows):
    df_frThrows = df_freeThrows


    #df_Stats = pd.read_csv('Seasons_Stats_v1.csv', sep=',', header=0, encoding='latin1')
    # split end_result to away_score and home_score
    # split game to away_team and home_team
    # split season to startYear and endYear, then delete all rows w startYear <= 2012
    df_frThrows['away_points'] = df_frThrows.end_result.str.split(' - ', expand=True)[0]
    df_frThrows['home_points'] = df_frThrows.end_result.str.split(' - ', expand=True)[1]
    df_frThrows['away_team'] = df_frThrows.game.str.split(' - ', expand=True)[0]
    df_frThrows['home_team'] = df_frThrows.game.str.split(' - ', expand=True)[1]
    df_frThrows['season_start'] = df_frThrows.season.str.split(' - ', expand=True)[0]
    df_frThrows['season_end'] = df_frThrows.season.str.split(' - ', expand=True)[1]
    df_frThrows[['season_start']] = df_frThrows[['season_start']].apply(pd.to_numeric)
    df_frThrows.apply(pd.to_numeric, errors='ignore')

    # so we can just work with season 2014-2015 and 2015-2016 data
    df_frThrows = df_frThrows[df_frThrows['season_start'] > 2013]

    # let's set the surrogate key of the fact now to start at 1
    surrogateKey = list(map(lambda x: x - 496532, df_frThrows.index.tolist()))

    df_frThrows.insert(loc=0, column='freeThrow_surrKey',
                       value=surrogateKey)

    # finally lets delete unneeded columns that we cleaned data from,
    # as well as dataframe indices and current score during freethrow data since we don't need it for this dataset
    df_frThrows = df_frThrows.drop(['end_result', 'game', 'season', 'score'], axis=1)


    # df_frThrows = df_frThrows[['fr', 'column I want second'...etc.]]
    if os.path.exists('free_throws_v1.csv'): os.remove('free_throws_v1.csv')
    df_frThrows.to_csv('free_throws_v1.csv', encoding='latin1', header=list(df_frThrows), index=False)
    return df_frThrows

def version2_freethrows(df_freeThrows):
    df_frThrows = df_freeThrows

    #let's get rid of unneeded words now
    df_frThrows['play'] = df_frThrows.play.str.split('makes|misses', expand=True)[1]
    df_frThrows['play'] = df_frThrows.play.str.replace(' free throw|of', '')

    #getting rid of whitespace
    df_frThrows['frThrow_num'] = df_frThrows.play.str.split('  ', expand=True)[0]
    df_frThrows['frThrow_totalnum'] = df_frThrows.play.str.split('  ', expand=True, )[1]
    df_frThrows.frThrow_num = df_frThrows.frThrow_num.str.replace(' ', '')

    #adding new column technical as  int(BOOL) pulling from frThrow_num
    df_frThrows['technical'] = df_frThrows['frThrow_num'] == 'technical'
    df_frThrows.technical = df_frThrows.technical.astype(int)

    #let's remove technical from frThrow_num and replace with 1
    df_frThrows.frThrow_num = df_frThrows.frThrow_num.str.replace('technical', '1')

    #finally lets clean up team names to match proper abbreviations from dict
    df_frThrows[['home_team']] = df_frThrows.home_team.str.replace('NO', 'NOP')
    df_frThrows[['home_team']] = df_frThrows.home_team.str.replace('SA', 'SAS')
    df_frThrows[['home_team']] = df_frThrows.home_team.str.replace('SASC', 'SAC')
    df_frThrows[['home_team']] = df_frThrows.home_team.str.replace('NY', 'NYK')
    df_frThrows[['home_team']] = df_frThrows.home_team.str.replace('UTAH', 'UTA')
    df_frThrows[['home_team']] = df_frThrows.home_team.str.replace('GS', 'GSW')

    df_frThrows[['away_team']] = df_frThrows.away_team.str.replace('NO', 'NOP')
    df_frThrows[['away_team']] = df_frThrows.away_team.str.replace('SA', 'SAS')
    df_frThrows[['away_team']] = df_frThrows.away_team.str.replace('SASC', 'SAC')
    df_frThrows[['away_team']] = df_frThrows.away_team.str.replace('NY', 'NYK')
    df_frThrows[['away_team']] = df_frThrows.away_team.str.replace('UTAH', 'UTA')
    df_frThrows[['away_team']] = df_frThrows.away_team.str.replace('GS', 'GSW')


    #only thing left is None in frThrow_totalnum for technical shots
    # lets replace this with 1 since NBA technical freethrows given = 1
    df_frThrows.fillna(value='1', inplace=True)

    #changing game_id to int, we won't need this column later when populating DB, but should keep for now
    #changing period to int as well
    df_frThrows.game_id = df_frThrows.game_id.astype(int)
    df_frThrows.period = df_frThrows.period.astype(int)

    #let's drop play column since we've extracted all necessary data fields now
    #lets rearrange columns from more relevant to less relevant

    df_frThrows = df_frThrows.drop(['play'], axis=1)
    df_frThrows = df_frThrows[['freeThrow_surrKey', 'player', 'shot_made', 'frThrow_num', 'frThrow_totalnum', 'technical',
                               'game_id', 'period', 'time', 'playoffs',
                               'home_team', 'home_points', 'away_team', 'away_points',
                               'season_start', 'season_end']]
    df_frThrows['game_id'] = pd.factorize(df_frThrows['game_id'])[0] + 1
    if os.path.exists('free_throws_v2.csv'): os.remove('free_throws_v2.csv')
    df_frThrows.to_csv('free_throws_v2.csv', encoding='latin1', header=list(df_frThrows), index=False)
    return df_frThrows

def version2_seasonStats(df_seasonStats):
    df_Stats = df_seasonStats

    #make year int type and delete all data <

    df_Stats[['Year']] = df_Stats[['Year']].apply(pd.to_numeric)
    df_Stats.Year = df_Stats.Year.astype(float)
    pd.options.display.float_format = '{:.0f}'.format

    # so we can just work with season 2014-2015 and 2015-2016 data
    # also lets delete rows with Tm = TOT; values can be found from other rows
    df_Stats[['Tm']] = df_Stats.Tm.str.replace('CHO', 'CHA')
    df_Stats = df_Stats[(df_Stats['Tm'] != 'TOT')]

    df_Stats = df_Stats[(df_Stats['Year'] >= 2014) & (df_Stats['Year'] <= 2016)]
    df_Stats.reset_index(drop=True, inplace=True)

    #lets rename and clean up columns to join stuff later
    df_Stats = df_Stats.rename(columns={'Player': 'player', 'Tm': 'team', 'Year': 'year', 'Age': 'age_2016'})



    df_Stats = df_Stats.drop(['G'], axis=1)

    #let's make all ages CURRENT as of 2016
    df_Stats[['age_2016']] = df_Stats[['age_2016']].apply(pd.to_numeric)
    df_Stats[['age_2016']] = (2016 - df_Stats['year']) + df_Stats['age_2016']


    #this is being replaced by doing some research on NBA teams as of now
    return df_Stats


if __name__ == '__main__':
    # to Test
    # if os.path.exists('free_throws_v1.csv'): os.remove('free_throws_v1.csv')

    if(not os.path.exists('free_throws_v1.csv')):
        df_freeThrows = pd.read_csv('free_throws.csv', sep=",", header=0, encoding='latin1')
        df_test = version1_freethrows(df_freeThrows)

    #to Test
    #if os.path.exists('free_throws_v2.csv'): os.remove('free_throws_v2.csv')

    if(not os.path.exists('free_throws_v2.csv')):
        df_freeThrows = pd.read_csv('free_throws_v1.csv', sep=",", header=0, encoding='latin1')
        df_test= version2_freethrows(df_freeThrows)

    # to Test
    #if os.path.exists('free_throws_v2.csv'): os.remove('free_throws_v2.csv')

    if not os.path.exists('Seasons_Stats_v2.csv'):
        df_seasonStats = pd.read_csv('Seasons_Stats_v1.csv', sep=",", header=0, encoding='latin1')


    df_seasonstats = version2_seasonStats(df_seasonStats)
    df_freethrows = pd.read_csv('free_throws_v2.csv', sep=",", header=0, encoding='latin1')

    players_freethrows = df_freethrows['player'].unique().tolist()
    players_stats = df_seasonstats['player'].unique().tolist()

    print(len(players_freethrows))
    print(len(players_stats))
    #
    # #let's see whats in freethrows not in player stats that we need to fix in stats to join properly
    differenceInNames = [player for player in players_freethrows if player not in players_stats]
    print(differenceInNames)
    #
    # #we will keep data consistent in freeThrows
    #
    #
    # # import re
    # #
    # # consequitivedots = re.compile(r'\.')
    # # consequitivedots.sub('', inputstring)2


    df_freethrows.player = df_freethrows.player.str.replace('Jeff Taylor', 'Jeffery Taylor')
    df_freethrows.player = df_freethrows.player.str.replace('Luc Richard', 'Luc Mbah')
    df_freethrows.player = df_freethrows.player.str.replace('Nene', 'Nene Hilario')
    df_freethrows.player = df_freethrows.player.str.replace("[\\.]", "")
    df_seasonstats.player = df_seasonstats.player.str.replace("[\\.]", "")
    #df_seasonstats.player = df_seasonstats.player.str.replace('\.', '', regex=True)

    #df['col2'] = (df['col2'].replace('\.', '', regex=True)

    df_seasonstats.player = df_seasonstats.player.str.replace('Lou Williams', 'Louis Williams')

    players_freethrows = df_freethrows['player'].unique().tolist()
    players_stats = df_seasonstats['player'].unique().tolist()

    print(len(players_freethrows))
    print(len(players_stats))
    #
    # #let's see whats in freethrows not in player stats that we need to fix in stats to join properly
    differenceInNames = [player for player in players_freethrows if player not in players_stats]
    print(differenceInNames)
    #lets extra players from seasonstats since we don't care about those
    diffFromStats = [player for player in players_stats if player not in players_freethrows]
    df_seasonstats = df_seasonstats[~df_seasonstats['player'].isin(diffFromStats)]


    players_freethrows = df_freethrows['player'].unique().tolist()
    players_stats = df_seasonstats['player'].unique().tolist()
    print(len(players_freethrows))
    print(len(players_stats))
    differenceInNames = [player for player in players_freethrows if player not in players_stats]


    merged = pd.merge(df_freethrows, df_seasonstats, how='left',
                                          left_on='player',
                                          right_on='player')


    merged = merged.drop_duplicates('freeThrow_surrKey')
    merged['player_home'] = merged['team'] == merged['home_team']
    merged.player_home = merged.player_home.astype(int)

    #getting team ids and player ids before loading data to keep data clean
    merged['playerteam_id'] = pd.factorize(merged['team'])[0] + 1
    merged['player_id'] = pd.factorize(merged['player'])[0] + 1


    merged = merged.rename(columns={'playoffs': 'season_type'})
    merged = merged.drop(['year'], axis=1)

    merged.frThrow_totalnum = pd.to_numeric(merged.frThrow_totalnum, errors='coerce')
    merged.frThrow_totalnum = merged.frThrow_totalnum.astype(float)
    pd.options.display.float_format = '{:.0f}'.format

    merged.frThrow_num = pd.to_numeric(merged.frThrow_num, errors='coerce')
    merged.frThrow_num = merged.frThrow_num.astype(float)
    pd.options.display.float_format = '{:.0f}'.format

    freeThrowsNumbers = merged.frThrow_totalnum.unique().tolist()
    weirdones = merged.loc[~merged['frThrow_num'].isin(freeThrowsNumbers)]

    print(merged.isna().any())

    #these are clearpath1/clearpath2/flagrant types etc. Will not be used for this
    merged = merged.dropna(subset=['frThrow_num'])

    #print(merged.isna().any())
    merged.columns = merged.columns.astype(str)

    #one-to-one function
    merged['freethrow_id']= merged['frThrow_num']*3 +merged['frThrow_totalnum']*2 + merged['technical']
    merged['freethrow_id'] = pd.factorize(merged['freethrow_id'])[0] + 1

    merged['time_minute'] = merged.time.str.split(':', expand=True)[0]
    merged['time_second'] = merged.time.str.split(':', expand=True, )[1]
    merged = merged.drop(['time'], axis=1)
    merged['periodtime_id'] = merged['period'].astype(float) + merged['time_minute'].astype(float) + merged['time_second'].astype(float)
    merged['periodtime_id'] = pd.factorize(merged['periodtime_id'])[0] + 1
    merged['season_id'] = merged['season_start'].astype(float) + merged['season_end'].astype(float) +(merged['season_type'] == 'playoffs')
    merged['season_id'] = pd.factorize(merged['season_id'])[0] + 1


    freeThrowsNumbers = merged.freethrow_id.unique().tolist()
    periodNumbers = merged.periodtime_id.unique().tolist()
    seasonNumbers = merged.season_id.unique().tolist()

    print(merged.isna().any())



    #FINALLY LETS EXPORT TO LOAD DATABASE!!!!!!!!!
    os.remove('freethrows_FINAL.csv')
    merged.to_csv('freethrows_FINAL.csv', encoding='latin1', header=list(merged), index=False)


    import pdb;pdb.set_trace()
