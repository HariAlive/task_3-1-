Task3_1
kaggle YT Data includes the following:

[USA, Britain, Germany, Canada, France, Russia, Mexico, Korea, Japan, India]

1. video title
2. channel title 
3. publish time 
4. tags
5. views
6. likes and dislikes 
7. description
8. comment count
9. The data also includes a category_id field, which varies between regions.

Required transformation:
1. combine the dataset of different regions into a table.
2. table need 9 of the above following contents.
3. Load the dataset using MYSQL.

#extraction of data:
import pandas as pd

#reading the required files as dataframes after downloading
df = pd.read_csv("C:\\Users\\Malavika Vipin\\Desktop\\Hari\\e5\\Task_3\\CAvideos.csv")
print(df)

#to check the column heads
df.columns
*out = Index(['video_id', 'trending_date', 'title', 'channel_title', 'category_id',
       'publish_time', 'tags', 'views', 'likes', 'dislikes', 'comment_count',
       'thumbnail_link', 'comments_disabled', 'ratings_disabled',
       'video_error_or_removed', 'description'],
      dtype='object')

#to delete the unwanted columns
df = df.loc[:,~df.columns.isin(['video_id', 'trending_date','thumbnail_link', 'comments_disabled', 'ratings_disabled',
       'video_error_or_removed'])]

#again checking the columns req:
df.columns
*out = Index(['title', 'channel_title', 'category_id', 'publish_time', 'tags',
       'views', 'likes', 'dislikes', 'comment_count', 'description'],
      dtype='object')

#Repeating the steps as much as required to clean each data sets and later joining them as one using the append()
df = df.append('dataframes')

**280401 rows × 10 columns --> dataframes combined

#saving the file as csv
df.to_csv("filecombined.csv")


#Reading the json file into a dataframe
df_1 = pd.read_json(r"C:\Users\Malavika Vipin\Downloads\CA_category_id.json")
print(df_1)

#normalizing the json file to extract all the embeded files inside and renaming the required column for proposed merger between the dataframes 
json_CA = pd.json_normalize(df_1['items'])
json_CA.rename(columns={'id':'category_id'}, inplace=True)
json_CA

#concating all the json files (repetative steps)
json_combined = pd.concat([json_CA,json_US])
json_combined

**218 rows × 6 columns --> json_combined

#saving the json file
json_combined.to_csv("jsoncombined.csv")

#merging the two dataframes
data = pd.merge(df, jsoncombined, how='right', on='category_id')
data

1949483 rows × 16 columns --> combined data

#Rechecking the column contents
data.columns
*out = Index(['title', 'channel_title', 'category_id', 'publish_time', 'tags',
       'views', 'likes', 'dislikes', 'comment_count', 'description',
       'Unnamed: 0', 'kind', 'etag', 'snippet.channelId', 'snippet.title',
       'snippet.assignable'],
      dtype='object')

#installing mysql connector
!pip install mysql-connector-python

import mysql.connector

#setting up the connection
mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="",
)

print(mydb)
mycursor = mydb.cursor(buffered=True)

#creating table using sql query
mycursor.execute('''create table task3_new (title varchar(256), channel_title varchar(256), category_id int(5) primary key, publish_time date, 
tags varchar(100), views int, likes int, dislikes int, comment_count int, description text, `Unnamed: 0` int, 
kind varchar(256), etag varchar(256), `snippet.channelId` varchar(256), `snippet.title` varchar(256), `snippet.assignable` boolean)''')

#loading data into the server {{**since the data is too large(3.2 GB) the proposed load has not yet happened**}}
for row in load.itertuples():
    mycursor.execute('''INSERT INTO task3_new (title, channel_title, category_id, publish_time, tags, 
    views, likes, dislikes, comment_count, description,`Unnamed: 0`, kind, etag, `snippet.channelId`, 
    `snippet.title`,`snippet.assignable`)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''',row.title, row.channel_title, row.category_id, row.publish_time, row.tags,
    row.views, row.likes, row.dislikes, row.comment_count, row.description, row.Unnamed: 0, row.kind, row.etag, row.snippet.channelId,
    row.snippet.title, row.snippet.assignable)
mycursor.commit()
