## INTRODUCTION
This project aims to develop a user-friendly Streamlit application that utilizes the Google API to extract information on a YouTube channel, stores it in a MongoDB database, migrates it to a SQL data warehouse, and enables users to search for channel details and join tables to view data in the Streamlit app and made accessible for analysis and exploration within the Streamlit app.

## INSTALLATION
To run this project, you need to install the following packages:
`````
pip install google-api-python-client
pip install pymongo
pip install pandas
pip install mysql-connector-python
pip install plotly_express==0.4.0
pip install streamlit
``````
## KEY TECHNOLOGIES AND SKILLS
* Python scripting
* Data Collection
* API integration
* Streamlit
* Plotly Express
* Data Management using MongoDB (Atlas) and MySQL

## APPROACH
1. Set up a Streamlit application using the python library "streamlit", which provides an easy-to-use interface for users to enter a YouTube channel ID, view channel details, and select channels to migrate.
2. Establish a connection to the YouTube API V3, which allows to retrieve channel and video data by utilizing the Google API client library for Python.
3. Store the retrieved data in a MongoDB data lake, as MongoDB is a suitable choice for handling unstructured and semi-structured data. This is done by firstly writing a method to retrieve the previously called api call and storing the same data in the database in 3 different collections.
4. Transferring the collected data from multiple channels namely the channels,videos and comments to a SQL data warehouse, utilizing a SQL database like MySQL or PostgreSQL for this purpose.
5. Utilize SQL queries to retrieve specific channel data based on user input.
4. The retrieved data is displayed within the Streamlit application, leveraging Streamlit's data visualization capabilities to create charts and graphs for users to analyze the data.
