from googleapiclient.discovery import build
import streamlit as st
from PIL import Image
import pandas as pd
import pymongo
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import mysql.connector
import plotly.express as px
#from IPython.display import JSON

st.set_page_config(
    page_title="Youtube Analysis App |By Divyalakhsmi",
    page_icon=':shark',
    layout="wide",
    initial_sidebar_state="expanded",
)
img=Image.open("youtube image.jpg")
st.image(img,width=500)
st.header("***:red[YOUTUBE DATA HARVESTING AND WAREHOUSING]***",divider='rainbow')

tab1,tab2,tab3,tab4,tab5 = st.tabs(["About","Channel Id", "Upload to MongoDB","transformation","view"])
tab1.markdown("## :blue[Domain] : ***Social Media***")
tab1.markdown("## :blue[Technologies used] : ***Python,MongoDB, Youtube Data API, MySql, Streamlit***")
tab1.markdown("## :blue[Overview] : ***Retrieving the Youtube channels data from the Google API, storing it in a MongoDB, migrating and transforming data into a SQL database,then querying the data and displaying it in the Streamlit app.***")
tab3.write("upload the data into MongoDB")

#connecting with mongoDB Server
client=MongoClient("mongodb+srv://divya:dance@cluster0.0uobdoq.mongodb.net/?retryWrites=true&w=majority")
db=client.capstone_project

#connecting with mysql database
mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="" ,
  database="youtube" )
mycursor = mydb.cursor(buffered=True)

#connecting with youtube API
api_key="AIzaSyCn_BkinBSmxEpv3KKi8Jco1tHFEQHIqzI"
youtube=build("youtube","v3",developerKey=api_key)

#FUNCTION TO GET CHANNEL STATISTICS

def get_channel_statistics(youtube,channel_ids):
    all_data=[]
    request=youtube.channels().list(
            part='snippet,contentDetails,statistics',
            id=channel_ids)
    response=request.execute()
    for i in range(len(response['items'])):
        data=dict(channel_name=response['items'][i]['snippet']['title'],
                  channel_id=response['items'][i]['id'],
                  Subscription_count=response['items'][i]['statistics']['subscriberCount'],
                  channel_view_count=response['items'][i]['statistics']['viewCount'],
                  channel_description=response['items'][i]['snippet']['description'],
                  playlist_id=response['items'][i]['contentDetails']['relatedPlaylists']['uploads'],
                  video_count=response['items'][i]['statistics']['videoCount'])
        all_data.append(data)
    return all_data

#FUNCTION TO GET VIDEO ID

def get_videoId(youtube,channel_ids):
     video_ids=[]
     response = youtube.channels().list(id=channel_ids, 
                                  part='contentDetails').execute()
     playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
     
     request = youtube.playlistItems().list(
               part='snippet,contentDetails', 
               maxResults=50,
               playlistId=playlist_id)
     response=request.execute()
     
     for i in range(len(response['items'])):
          video_ids.append(response['items'][i]['contentDetails']['videoId'])
     next_Page_Token=response.get('nextPageToken') 

     while next_Page_Token is not None:
          request = youtube.playlistItems().list(
               part='snippet,contentDetails', 
               maxResults=50,
               playlistId=playlist_id,
               pageToken=next_Page_Token)
          response=request.execute()
     
          for i in range(len(response['items'])):
                video_ids.append(response['items'][i]['contentDetails']['videoId'])  
          next_Page_Token=response.get('nextPageToken')       
     return video_ids

#FUNCTION TO GET VIDEO DETAILS

def get_video_details(youtube,video_ids):
    all_video_detail=[]

    for i in range(0,len(video_ids),50):
        request = youtube.videos().list(
            part="snippet, contentDetails, statistics",
               id=','.join(video_ids[i:i+50]))
        response = request.execute()
        
        for video in response['items']:
            video_stat={'snippet':['title','tag','description'],
                        'statistics':['viewCount','likeCount','dislikeCount','favouriteCount','commentCount'],
                        'contentDetails':['duration','caption']}
            video_info={}
            video_info['channel_name']=video['snippet']['channelTitle']
            video_info['channel_id']=video['snippet']['channelId']
            video_info['video_id']=video['id']
            video_info['thumbnails']=video['snippet']['thumbnails']['default']['url']
            video_info['video_published_date']=video['snippet']['publishedAt'][0:10]
            video_info['video_published_time']=video['snippet']['publishedAt'][11:19]
            for i in video_stat.keys():
                 for j in video_stat[i]:
                    try:
                           video_info[j]=video[i][j]
                    except:
                         video_info[j]=None
            all_video_detail.append(video_info)
            
    return all_video_detail

#FUNCTION TO GET COMMENT DETAILS

def get_comments_details(youtube, video_ids):
   
    all_comments = []
    
    for video_id in video_ids:
            request = youtube.commentThreads().list(
                part="snippet,replies",
                videoId=video_id)
            response = request.execute()

            for i in range(len(response['items'])):
                  data=dict(comment_id=response['items'][i]['id'],
                            video_id=response['items'][i]['snippet']['videoId'],
                            comment_Text=response['items'][i]['snippet']['topLevelComment']['snippet']['textOriginal'],
                            comment_author=response['items'][i]['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            comment_published_date=response['items'][i]['snippet']['topLevelComment']['snippet']['publishedAt'][0:10],
                            comment_published_time=response['items'][i]['snippet']['topLevelComment']['snippet']['publishedAt'][11:19])
                  all_comments.append(data)
            
    return all_comments

#FUNCTION TO GET CHANNEL NAMES FROM MONGODB

def get_ch_names():
     name_ch=[]
     for i in db.channel_details.find():
          name_ch.append(i['channel_name'])
     return name_ch

with tab2:
     ch_id=st.text_input("*Enter youtube channel id*")
     
     if ch_id and st.button("Extract data"):
          ch_detail=get_channel_statistics(youtube,ch_id)
          st.write(f'Extracted data from :blue["{ch_detail[0]["channel_name"]}"]channel')
          st.table(ch_detail)

with tab3: 
       
     if st.button("upload to MongoDB"):
          with st.spinner("Please wait for it......"):
               channel_details=get_channel_statistics(youtube,ch_id)
               channel_video_ids=get_videoId(youtube,ch_id)
               channel_video_detail=get_video_details(youtube,channel_video_ids)
               channel_comment_detail=get_comments_details(youtube, channel_video_ids)
               collection1=db.channel_details
               collection1.insert_many(channel_details)

               collection2=db.video_details
               collection2.insert_many(channel_video_detail)

               collection3=db.comment_details
               collection3.insert_many(channel_comment_detail)
               st.success("uploaded the datas into mongoDB successfully")
               st.balloons()

with tab4:
    st.markdown("##Select a channel to transfer the data into SQL")
    namesOfCh=get_ch_names()
    user_input=st.selectbox("Select channel", options=namesOfCh)

    def insert_into_channels():
            collection1= db.channel_details
            query = """INSERT INTO channels VALUES(%s,%s,%s,%s,%s,%s,%s)"""
                
            for i in collection1.find({"channel_name" : user_input},{'_id':0}):
                mycursor.execute(query,tuple(i.values()))
                mydb.commit()
                
    def insert_into_videos():
            collection2 = db.video_details
            query1 = """INSERT INTO videos VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""

            for i in collection2.find({"channel_name" : user_input},{"_id":0}):
                mycursor.execute(query1,tuple(i.values()))
                mydb.commit()

    def insert_into_comments():
            collection2 = db.video_details
            collection3 = db.comment_details
            query2 = """INSERT INTO comments VALUES(%s,%s,%s,%s,%s,%s)"""

            for vid in collection2.find({"channel_name" : user_input},{'_id' : 0}):
                for i in collection3.find({'video_id': vid['video_id']},{'_id' : 0}):
                    #t=tuple(i.values())
                    mycursor.execute(query2,tuple(i.values()))
                    mydb.commit()

    if st.button("Submit"):
        try:
                
            insert_into_channels()
            insert_into_videos()
            insert_into_comments()
            st.success("Transformation to MySQL Successful!!!")
            st.balloons()
        except:
            st.error("Channel details already transformed!!")

with tab5:
        
        st.write("## :green[Select any question to get the insights]")
        qn=st.selectbox('questions',
                        ['Click one question that you would like to query',
                        '1. What are the names of all the videos and their corresponding channels?',
                        '2. Which channels have the most number of videos, and how many videos do they have?',
                        '3. What are the top 10 most viewed videos and their respective channels?',
                        '4. How many comments were made on each video, and what are their corresponding video names?',
                        '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
                        '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
                        '7. What is the total number of views for each channel, and what are their corresponding channel names?',
                        '8. What are the names of all the channels that have published videos in the year 2022?',
                        '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
                        '10.Which videos have the highest number of comments, and what are their corresponding channel names?',
                        ])
        
        if qn=='1. What are the names of all the videos and their corresponding channels?':
            mycursor.execute("SELECT channel_name,title AS video_name from videos order by channel_name")
            df=pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
            st.write(df)

        elif qn == '2. Which channels have the most number of videos, and how many videos do they have?':
            mycursor.execute("SELECT channel_name,video_count from channels order by video_count desc")
            df=pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
            st.write(df)   
            st.write("### :green[Number of videos in each channel :]")
            graph = px.bar(df,
                     x=mycursor.column_names[0],
                     y=mycursor.column_names[1],
                     orientation='v',
                     color=mycursor.column_names[0]
                    )
            st.plotly_chart(graph,use_container_width=True)

        elif qn == '3. What are the top 10 most viewed videos and their respective channels?':
            mycursor.execute("SELECT channel_name,title AS video_name,viewCount from videos order by viewCount DESC limit 10")
            df=pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
            st.write(df)
            st.write("### :green[Top 10 most viewed videos :]")
            graph = px.bar(df,
                     x=mycursor.column_names[2],
                     y=mycursor.column_names[1],
                     orientation='h',
                     color=mycursor.column_names[0]
                    )
            st.plotly_chart(graph,use_container_width=True) 

        elif qn == '4. How many comments were made on each video, and what are their corresponding video names?':
            mycursor.execute("select channel_name,title as video_name,commentCount from videos ")
            df=pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
            st.write(df)

        elif qn == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
            mycursor.execute("select channel_name,title as video_name,likeCount from videos order by likeCount desc limit 10")
            df=pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
            st.write(df)
            st.write("### :green[Top 10 most liked videos :]")
            graph = px.bar(df,
                     x=mycursor.column_names[2],
                     y=mycursor.column_names[1],
                     orientation='h',
                     color=mycursor.column_names[0]
                    )
            st.plotly_chart(graph,use_container_width=True)

        elif qn == '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
            mycursor.execute("select channel_name,title as video_name,likeCount,dislikeCount from videos order by likeCount")
            df=pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
            st.write(df)

        elif qn == '7. What is the total number of views for each channel, and what are their corresponding channel names?':
             mycursor.execute("select channel_name,channel_view_count from channels")
             df=pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
             st.write(df)
             st.write("### :green[Channels vs Views :]")
             graph = px.bar(df,
                     x=mycursor.column_names[0],
                     y=mycursor.column_names[1],
                     orientation='v',
                     color=mycursor.column_names[0]
                    )
             st.plotly_chart(graph,use_container_width=True)

        elif qn == '8. What are the names of all the channels that have published videos in the year 2022?':
             mycursor.execute("select channel_name,video_published_date from videos where video_published_date like '2022%' group by channel_name")
             df=pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
             st.write(df)
             st.write("### :green[channel published video in the year 2022 :]")
             graph = px.bar(df,
                     x=mycursor.column_names[0],
                     y=mycursor.column_names[1],
                     orientation='v',
                     color=mycursor.column_names[0]
                    )
             st.plotly_chart(graph,use_container_width=True)


        elif qn == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
             mycursor.execute("""SELECT channel_name, 
                        SUM(duration_sec) / COUNT(*) AS average_duration
                        FROM (
                            SELECT channel_name, 
                            CASE
                                WHEN duration REGEXP '^PT[0-9]+H[0-9]+M[0-9]+S$' THEN 
                                TIME_TO_SEC(CONCAT(
                                SUBSTRING_INDEX(SUBSTRING_INDEX(duration, 'H', 1), 'T', -1), ':',
                            SUBSTRING_INDEX(SUBSTRING_INDEX(duration, 'M', 1), 'H', -1), ':',
                            SUBSTRING_INDEX(SUBSTRING_INDEX(duration, 'S', 1), 'M', -1)
                            ))
                                WHEN duration REGEXP '^PT[0-9]+M[0-9]+S$' THEN 
                                TIME_TO_SEC(CONCAT(
                                '0:', SUBSTRING_INDEX(SUBSTRING_INDEX(duration, 'M', 1), 'T', -1), ':',
                                SUBSTRING_INDEX(SUBSTRING_INDEX(duration, 'S', 1), 'M', -1)
                            ))
                                WHEN duration REGEXP '^PT[0-9]+S$' THEN 
                                TIME_TO_SEC(CONCAT('0:0:', SUBSTRING_INDEX(SUBSTRING_INDEX(duration, 'S', 1), 'T', -1)))
                                END AS duration_sec
                        FROM videos
                        ) AS subquery
                        GROUP BY channel_name""")
             df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
             st.write(df)
             st.write("### :green[Average duration of the channels :]")
             graph = px.bar(df,
                     x=mycursor.column_names[0],
                     y=mycursor.column_names[1],
                     orientation='v',
                     color=mycursor.column_names[0]
                    )
             st.plotly_chart(graph,use_container_width=True)


        elif qn == '10.Which videos have the highest number of comments, and what are their corresponding channel names?':
             mycursor.execute("select channel_name,title as video_name,commentCount from videos order by commentCount desc limit 10")
             df=pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
             st.write(df)
             st.write("### :green[Videos with most comments :]")
             graph = px.bar(df,
                     x=mycursor.column_names[1],
                     y=mycursor.column_names[2],
                     orientation='v',
                     color=mycursor.column_names[0]
                    )
             st.plotly_chart(graph,use_container_width=True)
