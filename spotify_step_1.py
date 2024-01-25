import json
import os
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import boto3
from datetime import datetime

def lambda_handler(event, context):
    client_id= os.environ.get("client_id")
    client_secret = os.environ.get("client_secret")
    credentials_master = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
    sp_object = spotipy.Spotify(client_credentials_manager=credentials_master)
    playlist_url = "https://open.spotify.com/playlist/37i9dQZEVXbMDoHDwVN2tF"
    playlist_link = playlist_url.split("/")[-1]
    data = sp_object.playlist_tracks(playlist_link)
    #Here I'm interested in knowing the markets where this specific album is available. So, I deleted the track specific market availability
    for row in data["items"]:
            # Remove the "available_markets" key-pair
        if "available_markets" in row["track"].keys():
            del row["track"]["available_markets"]
        if "images" in row["track"]["album"].keys():
            del row["track"]["album"]["images"]
    client = boto3.client("s3")
    
    filename = "spotify_raw_data" + str(datetime.now()) + ".json"
    client.put_object(
        Bucket = "spotify-project-ljakka2",
        Key= "raw_data/to_process/"+ filename,
        Body= json.dumps(data))
