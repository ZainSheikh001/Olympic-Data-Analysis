from tenacity import retry_unless_exception_type

import numpy as np


## Now we will make the functoin that will fecth the country and year of olympics
def fetch_medal_tally(df, year, country):
    medal_df = df.drop_duplicates(subset=['Team', 'NOC', 'Games', 'Year', 'City', 'Sport', 'Event', 'Medal'])
    flag = 0
    if year == 'Overall' and country == 'Overall':
        temp_df = medal_df
    if year == 'Overall' and country != 'Overall':
        flag = 1
        temp_df = medal_df[medal_df['region'] == country]
    if year != 'Overall' and country == 'Overall':
        temp_df = medal_df[medal_df['Year'] == int(year)]
    if year != 'Overall' and country != 'Overall':
        temp_df = medal_df[(medal_df['Year'] == year) & (medal_df['region'] == country)]


    if flag == 1:
        x = temp_df.groupby('Year')[['Gold', 'Silver', 'Bronze']].sum().sort_values('Year',
                                                                                    ascending=True).reset_index()
    else:
        x = temp_df.groupby('region')[['Gold', 'Silver', 'Bronze']].sum().sort_values('Gold',
                                                                                      ascending=False).reset_index()
    x['total'] = x['Gold'] + x['Silver'] + x['Bronze']
    x['Gold'] = x['Gold'].astype(int)
    x['Silver'] = x['Silver'].astype(int)
    x['Bronze'] = x['Bronze'].astype(int)
    x['total'] = x['total'].astype(int)
    return(x)


def medal_tally(df):
    medal_tally = df.drop_duplicates(subset=['Team', 'NOC', 'Games', 'Year', 'City', 'Sport', 'Event', 'Medal'])
    medal_tally = medal_tally.groupby('region')[['Gold', 'Silver', 'Bronze']].sum().sort_values('Gold', ascending=False).reset_index()
    medal_tally['total'] = medal_tally['Gold'] + medal_tally['Silver'] + medal_tally['Bronze']
    medal_tally['Gold'] = medal_tally['Gold'].astype(int)
    medal_tally['Silver'] = medal_tally['Silver'].astype(int)
    medal_tally['Bronze'] = medal_tally['Bronze'].astype(int)
    medal_tally['total'] = medal_tally['total'].astype(int)

    return medal_tally
def country_year_list(df):
    years = df['Year'].unique().tolist()
    years.sort()
    years.insert(0, 'Overall')

    country = np.unique(df['region'].dropna().tolist())
    country.sort()
    country = np.insert(country, 0, 'Overall')
    return years,country


def data_over_time(df, col):
    # Drop duplicates for Year + col (to avoid double counting)
    data_over_time = (
        df.drop_duplicates(['Year', col])
          .groupby('Year')
          .size()
          .reset_index(name='Count')  # Always use Count for y-axis
    )

    # Semantic year column
    data_over_time.rename(columns={'Year': 'Edition'}, inplace=True)
    return data_over_time

# helper.py
def most_successful(df, sport):
    # 1. Keep only medal winners
    temp_df = df.dropna(subset=['Medal'])

    # 2. If a specific sport is selected, filter by it
    if sport != 'Overall':
        temp_df = temp_df[temp_df['Sport'] == sport]

    # 3. Group by Name, Region, Sport → count medals
    result = (
        temp_df.groupby(['Name', 'Team', 'Sport'])['Medal']
        .count()
        .reset_index(name='Medals')
        .sort_values(by='Medals', ascending=False, ignore_index=True)  # 👈 here
        .head(15)
    )
    # top 15
    result.rename(columns={'Team': 'Country'}, inplace=True)


    return result

def yearwise_medal_tally(df,country):
    temp_df = df.dropna(subset=['Medal'])
    temp_df.drop_duplicates(subset=['Team', 'NOC', 'Games', 'Year', 'City', 'Sport', 'Event', 'Medal'], inplace=True)

    new_df = temp_df[temp_df['region'] == country]
    final_df = new_df.groupby('Year').count()['Medal'].reset_index()

    return final_df

def country_event_heatmap(df, country):
    temp_df = df.dropna(subset=['Medal'])
    temp_df = temp_df.drop_duplicates(
        subset=['Team', 'NOC', 'Games', 'Year', 'City', 'Sport', 'Event', 'Medal']
    )

    new_df = temp_df[temp_df['region'] == country]

    # pivot_table will return empty DF if no data, so no crash
    pt = new_df.pivot_table(
        index='Sport',
        columns='Year',
        values='Medal',
        aggfunc='count'
    ).fillna(0).astype(int)

    return pt


def most_successful_country_wise(df, country):
    # 1. Keep only medal winners
    temp_df = df.dropna(subset=['Medal'])

    # 2. Filter by selected country
    temp_df = temp_df[temp_df['region'] == country]

    # 3. Group by Name, Sport → count medals
    result = (
        temp_df.groupby(['Name', 'Sport'])['Medal']
        .count()
        .reset_index(name='Medals')
        .sort_values(by='Medals', ascending=False, ignore_index=True)
        .head(10)
    )
    return result
def weight_v_height(df,sport):
    athlete_df = df.drop_duplicates(subset=['Name', 'region'])
    athlete_df['Medal'].fillna('No Medal', inplace=True)
    if sport != 'Overall':
        temp_df = athlete_df[athlete_df['Sport'] == sport]
        return temp_df
    else:
        return athlete_df

def men_vs_women(df):
    athlete_df = df.drop_duplicates(subset=['Name', 'region'])

    men = athlete_df[athlete_df['Sex'] == 'M'].groupby('Year').count()['Name'].reset_index()
    women = athlete_df[athlete_df['Sex'] == 'F'].groupby('Year').count()['Name'].reset_index()

    final = men.merge(women, on='Year', how='left')
    final.rename(columns={'Name_x': 'Male', 'Name_y': 'Female'}, inplace=True)

    final.fillna(0, inplace=True)

    return final
