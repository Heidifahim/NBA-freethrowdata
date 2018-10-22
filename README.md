<h1>NBA-freethrowdata<h1>
<h4>Extracting data from https://www.kaggle.com/sebastianmantey/nba-free-throws <br>
and https://www.kaggle.com/drgilermo/nba-players-stats to 
look at some interesting freethrow-related stats.</h4>

<br>**1)/data_cleanup/:**
* contains all the csvs and the different versions I created/worked with
* nba_datastaging.py 
  * contains all the code to clean up and merge both csvs into a final csv ready to load into a database
  * **freethrows_FINAL.csv** is the final version ready to load into DB
