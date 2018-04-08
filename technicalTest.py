import pandas as pd
import datetime as dt
import numpy as np
import glob


# Specify path of folder with all of the audit files
file_path = 'C:\\Users\\kesma\\Documents\\Python\\BigData\\userid-timestamp-artid-artname-traid-traname.tsv'

# You can execute each of the Parts A, B, C separately
# Parts A and B are complete
# Part C is not complete though I provided more information of how I would tackle it


# Part A
df = pd.read_csv(file_path, sep='\t', error_bad_lines=False, header=None, usecols=[0,5])
# Name for columns 0 and 5
df.columns = ['UserID','TrackName']
# Group by user IDs, and track names being unique / distinct
part_A = df.groupby(['UserID'])['TrackName'].nunique()
# Print results
print(part_A)
del part_A


# Part B
df = pd.read_csv(file_path, sep='\t', error_bad_lines=False, header=None, usecols=[3,5])
# Name for columns 3 and 5
df.columns = ['ArtistName','TrackName']
# Most popular songs means, Count the times they were played
part_B = df.groupby(['ArtistName','TrackName']).size().reset_index(name="Count")
# Sorting in Descending order. The most popular songs are the ones with the highest Count
part_B = part_B.sort_values(by='Count', ascending=False)
part_B = part_B.iloc[0:100, :]
print(part_B)
del part_B


# Part C
session = pd.read_csv(file_path, sep='\t', error_bad_lines=False, header=None, usecols=[0,1,5])
# Assign column names to columns 0 and 1
session.columns = ['UserID','TimeStamp','Song']
# Convert TimeStamp datatype object to datetime64
session['TimeStamp'] = pd.to_datetime(session['TimeStamp'])
# Sorting TimeStamp column
session = session.sort_values(by='TimeStamp', ascending=False)
# Calculate the difference between dataframe indices
session['delta'] = ((session['TimeStamp'] - session['TimeStamp'].shift()).fillna(0)).astype('timedelta64[m]')
# Make this number positive
session['timeDiff'] = (session['delta']*(-1))
# If more timeDiff > 20, the song will not be including in the session
session = session[session.timeDiff <= 20]
# session = session[session.timeDiff <= 20 and UserID = user_000001]

print(session)
del session

# Please read:
# To create a list of top 10 longest sessions, once computed the difference between the dataframe indices,
# I would just keep the rows where the timeDiff <= 20. Then, I would group by the records by UserIDs and
# add up all the outstanding timeDiff, and create a new column called 'totaltimeDiff'. Sort totaltimeDiff
# by descending order to give you the top 10 longest sessions.

