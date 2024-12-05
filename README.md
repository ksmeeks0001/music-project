# music-project
This project demonstrates the use of Airflow to automate data collection from scraping and pulling from an API. 

I first pulled the latest Billboard Hot 100 Artists chart weekly to get the list of artists. Then I used the Spotify API to get the artist's follower count. I was able to find a repo that handled scraping of the Billboard charts. The project uses the Python requests library to get the HTML and parse it using Beautiful Soup. I did have to make a modification to this project due to being blocked by Billboard's website. I added the ability to pass headers to the ChartData class that will be used by the requests library. This allowed be to pass a user-agent header to bypass the blocking. My fork can be found [here](https://github.com/ksmeeks0001/billboard-charts). 

After I was able to retreive the data, I needed to be able to store it. I knew I would also be pulling from the Spotify API so I went ahead and designed it for that data as well. The SQL definition can be found in [this](https://github.com/ksmeeks0001/music-project/blob/main/music-db.sql) repo. 

![Database Schema](https://raw.githubusercontent.com/ksmeeks0001/music-project/refs/heads/main/music-db.png)

I set this database up as a MySQL RDS instance on AWS. To automate the pulling of the data I used airflow. To deploy airflow I used a free tier EC2 instance with Ubuntu and installed airflow myself. The webserver and scheduler were set up as services to keep running. 

The [Billboard chart dag](https://github.com/ksmeeks0001/music-project/blob/main/dags/billboard-pull.py) is scheduled weekly on Tuesdays to pick up the latest chart. It uses a single PythonOperator task that utilizes the billboard-chart.py project. 

The last part of this project used the [Spotify API](https://developer.spotify.com/documentation/web-api) to pull the the artist's follower count and information of the artist's genre. I wrote a bare minimum [wrapper](https://github.com/ksmeeks0001/SpotifyAPI) around the API that handles authentication and 2 endpoints, the search to get an artists ID by name, and the artist endpoint to get the details after retrieving the ID. The [Spotify Dag](https://github.com/ksmeeks0001/music-project/blob/main/dags/spotify-pull.py) uses to PythonOperator tasks to utilize these 2 endpoints. 

Finally, I put together a simple [app](https://github.com/ksmeeks0001/music-project/blob/main/app.py) using Dash. The app allows you to select an artist and it will display 3 charts. The first showing Spotify followers over time, the second showing weekly Billboard chart position, and the third shows how much change in Spotify followers the artist received (the difference between today's follower count and yesterday's).

![app1](https://raw.githubusercontent.com/ksmeeks0001/music-project/refs/heads/main/app1.PNG)
![app2](https://raw.githubusercontent.com/ksmeeks0001/music-project/refs/heads/main/app2.PNG)
