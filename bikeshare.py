from calendar import c, month
from genericpath import isfile
from pickle import TRUE
import time
import pandas as pd
import numpy as np
from datetime import datetime ,timedelta
import os.path

CITY_DATA = { 'cities' : ['Chicago' , 'New York City' ,'Washington'],
              'cities_data_file' : ['./chicago.csv','./new_york_city.csv','./washington.csv']}

li_CD = []
li_cities = []
li_months = []
li_lastdays = []
li_days_of_week  = []

intervals = (
    ('months', 2419200),    # 60 * 60 * 24 * 7 * 4
    ('weeks', 604800),      # 60 * 60 * 24 * 7
    ('days', 86400),        # 60 * 60 * 24
    ('hours', 3600),        # 60 * 60
    ('minutes', 60),
    ('seconds', 1),
)

print('\nPre loading all data...\n')
start_time = time.time()

df_CITY_DATA = pd.DataFrame(CITY_DATA)
li_cities = df_CITY_DATA['cities'].values.tolist()

if os.path.isfile('store_combined.pkl'): 
    df_cd_readData = pd.read_pickle('store_combined.pkl')
else:                
    li_cities = df_CITY_DATA['cities'].values.tolist()

    for index in df_CITY_DATA.index:
        df_CD = pd.read_csv(df_CITY_DATA['cities_data_file'][index], index_col=None ,header=0)
        df_CD['Start Time'] = pd.to_datetime(df_CD['Start Time'])
        df_CD['End Time'] = pd.to_datetime(df_CD['End Time'])
        df_CD['Start Hour'] = df_CD['Start Time'].dt.hour
        df_CD['End Hour'] = df_CD['End Time'].dt.hour
        df_CD['Total Time sec'] = df_CD["End Time"] - df_CD["Start Time"]
        df_CD['Total Time sec'] = df_CD['Total Time sec']/np.timedelta64(1,'s') 
        df_CD['Short Day of Week'] = df_CD[['Start Time']].apply(lambda x: datetime.strftime(x['Start Time'], '%a'), axis=1)
        df_CD['Day of Week'] = df_CD[['Start Time']].apply(lambda x: datetime.strftime(x['Start Time'], '%A'), axis=1)
        df_CD['Short Month Name'] = df_CD[['Start Time']].apply(lambda x: datetime.strftime(x['Start Time'], '%b'), axis=1)
        df_CD['Month Name'] = df_CD[['Start Time']].apply(lambda x: datetime.strftime(x['Start Time'], '%B'), axis=1)
        df_CD.insert(0,"city",df_CITY_DATA['cities'][index],True)
        li_CD.append(df_CD)
        
    df_cd_comb = pd.concat(li_CD, axis=0 , sort=True, ignore_index=True)
    df_cd_comb['Gender'] = df_cd_comb['Gender'].fillna('unknown')    
    df_cd_comb['Birth Year'] = df_cd_comb['Birth Year'].fillna(0)
    df_cd_comb['Birth Year'] = df_cd_comb['Birth Year'].astype(int)

    df_cd_comb.to_pickle('store_combined.pkl')
    df_cd_readData = pd.read_pickle('store_combined.pkl')
    
df_cd_combined = pd.DataFrame(df_cd_readData)

min_date = df_cd_combined['Start Time'].min()
max_date = df_cd_combined['Start Time'].max()

datetimestamps = pd.date_range(min_date,max_date,freq='M')
week_days = pd.date_range(min_date,max_date,freq='D').strftime('%a').unique()

li_months = datetimestamps.strftime('%b').values.tolist()
li_months.insert(0,'All')


lastdays = datetimestamps.strftime('%d').values.tolist()

li_days_of_week = week_days.values.tolist()
li_days_of_week.insert(0,'All')


print("\nThis took %s seconds." % (time.time() - start_time))
print('-'*40)
    
def get_filters():
  
    """
    Asks user to specify a city, month, and day to analyze.

    Returns:
        (str) city - name of the city to analyze
        (str) month - name of the month to filter by, or "all" to apply no month filter
        (str) day - name of the day of week to filter by, or "all" to apply no day filter
    """
    city=''
    month=''
    day = '' 
    
    print('Hello! Let\'s explore some US bikeshare data!')

    while city.lower() not in [cities.lower() for cities in li_cities]:
        city = input('Please select the city you would likte to analyze eg. ({}) : '.format(', '.join(li_cities)))

    while month.lower() not in [months.lower() for months in li_months]:
        month = input('Please select the month you would like to analyze eg. ({}) : '.format(', '.join(li_months)))

    while day.lower() not in [days.lower() for days in li_days_of_week]:
        day = input('Please select the month you would like to analyze eg. ({}) : '.format(', '.join(li_days_of_week)))
        
    print('-'*40)
    return city.title(), month.title(), day.title()


def load_data(city, month, day):
    """
    Loads data for the specified city and filters by month and day if applicable.

    Args:
        (str) city - name of the city to analyze
        (str) month - name of the month to filter by, or "all" to apply no month filter
        (str) day - name of the day of week to filter by, or "all" to apply no day filter
    Returns:
        df - Pandas DataFrame containing city data filtered by month and day
    """
    if month != 'All' and day != 'All':
        df = df_cd_combined.loc[(df_cd_combined['city'] == city) & 
                             (df_cd_combined['Short Month Name'] == month) & 
                             (df_cd_combined['Short Day of Week'] == day)]
    elif month == 'All' and day != 'All':
        df = df_cd_combined.loc[(df_cd_combined['city'] == city) &
                             (df_cd_combined['Short Day of Week'] == day)]
    elif month != 'All' and day == 'All':
        df = df_cd_combined.loc[(df_cd_combined['city'] == city) & 
                             (df_cd_combined['Short Month Name'] == month)]
    elif month == 'All' and day == 'All': 
        df = df_cd_combined.loc[(df_cd_combined['city'] == city)]
    return df


def time_stats(df):
    """Displays statistics on the most frequent times of travel."""

    print('\nCalculating The Most Frequent Times of Travel...\n')
    start_time = time.time()

    most_common_month = df['Month Name'].value_counts().idxmax()
    print("\nThe most common month is - {}.".format(most_common_month))
    
    most_common_dow = df['Day of Week'].value_counts().idxmax()
    print("\nThe most common day of week is - {}.".format(most_common_dow))

    most_common_start_time = df['Start Hour'].value_counts().idxmax()
    print("\nThe most common start hour is - {}.".format(most_common_start_time))
    
    most_common_end_time = df['End Hour'].value_counts().idxmax()
    print("\nThe most common end hour is - {}.".format(most_common_end_time))

    print("\nThis took %s seconds." % (time.time() - start_time))
    print('-'*40)


def station_stats(df):
    """Displays statistics on the most popular stations and trip."""

    print('\nCalculating The Most Popular Stations and Trip...\n')
    start_time = time.time()

    most_common_start_station = df['Start Station'].value_counts().idxmax()
    print("\nThe most common start station is - {}.".format(most_common_start_station))

    most_common_end_station = df['End Station'].value_counts().idxmax()
    print("\nThe most common end station is - {}.".format(most_common_end_station))

    most_frequent_combination = (df['Start Station'] + ' - ' + df['End Station']).mode().values[0]
    print("\nThe most frequent combination of start station and end station is - {}.".format(most_frequent_combination))

    print("\nThis took %s seconds." % (time.time() - start_time))
    print('-'*40)


def trip_duration_stats(df):
    """Displays statistics on the total and average trip duration."""

    print('\nCalculating Trip Duration...\n')
    start_time = time.time()

    total_travel_time = display_time(df['Total Time sec'].sum(),6)
    print("\nThe total travel time was - {}.".format(total_travel_time))

    mean_travel_time = display_time(df['Total Time sec'].mean(),6)
    print("\nThe mean travel time was - {}.".format(mean_travel_time))

    print("\nThis took %s seconds." % (time.time() - start_time))
    print('-'*40)


def user_stats(df):
    """Displays statistics on bikeshare users."""

    print('\nCalculating User Stats...\n')
    start_time = time.time()

    print("\n",df.groupby(['User Type']).size().to_frame('count').reset_index())
    print("\n",df.groupby(['Gender']).size().to_frame('count').reset_index())
    
    earliest_year_of_birth = df.loc[df['Birth Year']>0, 'Birth Year'].min()
    print("\nThe earliest year of birth is - {}.".format(earliest_year_of_birth))
    
    recent_year_of_birth = df.loc[df['Birth Year']>0, 'Birth Year'].max()
    print("\nThe most recent year of birth is - {}.".format(recent_year_of_birth))
    
    most_common_year_of_birth = df.loc[df['Birth Year']>0, 'Birth Year'].value_counts().idxmax()
    print("\nThe most common year of birth is - {}.".format(most_common_year_of_birth))

    print("\nThis took %s seconds." % (time.time() - start_time))
    print('-'*40)

def display_time(seconds, granularity=2):
    """ Convert Seconds to months - weeks - days - hours - minutes - seconds

    Args:
        seconds (_int_): seconds that needs to be converted
        granularity (int, optional): granularity of output values. Defaults to 2.

    Returns:
        intervals : Output according to granularity int intervals
    """
    result = []

    for name, count in intervals:
        value = seconds // count
        if value:
            seconds -= value * count
            if value == 1:
                name = name.rstrip('s')
            result.append("{} {}".format(value, name))
    return ', '.join(result[:granularity])

def main():
    while True:
        city, month, day = get_filters() 
        df = load_data(city, month, day)

        time_stats(df)
        station_stats(df)
        trip_duration_stats(df)
        user_stats(df)

        restart = input('\nWould you like to restart? Enter yes or no.\n')
        if restart.lower() != 'yes':
            break


if __name__ == "__main__":
	main()
