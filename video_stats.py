import requests
import json
import os
from dotenv import load_dotenv
from datetime import date

load_dotenv(dotenv_path="./.env")
API_KEY = os.getenv("API_KEY")
CHANNEL_HANDLE = "mariocaesarKA"
maxResults = 50

def get_channel_playlistId():
    try :
        url = f"https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={CHANNEL_HANDLE}&key={API_KEY}"
        response = requests.get(url)

        response.raise_for_status()  # Check if the request was successful

        data = response.json()

        channel_items = data['items'][0]

        channel_playlistId = channel_items['contentDetails']['relatedPlaylists']['uploads']

        # print(channel_playlistId)

        return channel_playlistId
    
    except requests.exceptions.RequestException as e:
        raise e

def get_video_ids(playlistId):
    base_url = f"https://youtube.googleapis.com/youtube/v3/playlistItems?part=contentDetails&maxResults={maxResults}&playlistId={playlistId}&key={API_KEY}"
    video_ids = []
    pageToken = None

    try:
        while True:
            url = base_url
            if pageToken:
                url += f"&pageToken={pageToken}" # untuk pergi ke halaman berikutnya
            
            # Mengirim permintaan GET ke API YouTube
            response = requests.get(url)
            response.raise_for_status() 
            data = response.json()

            # Melakukan iterasi pada setiap item dalam respons untuk mendapatkan videoId
            for item in data.get('items', []):
                video_id = item['contentDetails']['videoId']
                video_ids.append(video_id)

            pageToken = data.get('nextPageToken')  # Mendapatkan token untuk halaman berikutnya

            if not pageToken:  # Jika tidak ada token, berarti sudah mencapai halaman terakhir
                break

        return video_ids

    except requests.exceptions.RequestException as e:
        raise e

def extract_video_data(video_ids):
    extracted_data = []

    def batch_list_video(video_id_list, batch_size):
        for i in range(0, len(video_id_list), batch_size):
            yield video_id_list[i:i + batch_size]
    
    try:
        for batch in batch_list_video(video_ids, maxResults):
            video_ids_str = ",".join(batch)
            
            url = f"https://youtube.googleapis.com/youtube/v3/videos?part=contentDetails&part=snippet&part=statistics&id={video_ids_str}&key={API_KEY}"

            response = requests.get(url)
            response.raise_for_status()
            data = response.json()

            for item in data.get('items', []):
                video_id = item['id']
                snippet = item['snippet']
                content_details = item['contentDetails']
                statistics = item['statistics']

                video_data = {
                    "video_id": video_id,
                    "title": snippet['title'],
                    "publishedAt": snippet['publishedAt'],
                    "duration": content_details['duration'],
                    "viewCount": statistics.get('viewCount', None),
                    "likeCount": statistics.get('likeCount', None),
                    "commentCount": statistics.get('commentCount', None)
                }

                extracted_data.append(video_data)

        return extracted_data

    except requests.exceptions.RequestException as e:
        raise e

def save_to_json(extracted_data):
    file_path = f"./data/{CHANNEL_HANDLE}_data_{date.today()}.json"

    with open(file_path, "w", encoding="utf-8") as json_outfile:
        json.dump(extracted_data, json_outfile, ensure_ascii=False, indent=4)

if __name__ == "__main__":
    playlistId = get_channel_playlistId()
    video_ids = get_video_ids(playlistId)
    videoData = extract_video_data(video_ids)
    save_to_json(videoData)