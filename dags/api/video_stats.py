import requests
import json

from airflow.models import Variable
from datetime import date
from airflow.decorators import dag, task


API_KEY = Variable.get("API_KEY")
CHANNEL_HANDLE = Variable.get("CHANNEL_HANDLE")
MAX_RESULTS = 50

@task
def get_playlist_id():
    try:
        url = f"https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={CHANNEL_HANDLE}&key={API_KEY}"

        response = requests.get(url)
        response.raise_for_status()

        data = response.json()
        # print(json.dumps(data, indent=4))

        channel_items = data["items"][0]
        channel_playlist_id = channel_items["contentDetails"]["relatedPlaylists"]["uploads"]

        print(channel_playlist_id)
        return channel_playlist_id
    except requests.exceptions.RequestException as e:
        raise e

@task
def get_video_ids(playlist_id):
    video_ids = []
    page_token = None
    base_url =  f"https://youtube.googleapis.com/youtube/v3/playlistItems?part=contentDetails&maxResults={MAX_RESULTS}&playlistId={playlist_id}&key={API_KEY}"

    try:
        while True:
            url = base_url
            if page_token:
                url += f"&pageToken={page_token}"
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            for item in data.get("items", []):
                video_ids.append(item["contentDetails"]["videoId"])
            page_token = data.get("nextPageToken")
            if not page_token:
                break
        return video_ids
    except requests.exceptions.RequestException as e:
        raise e

@task
def extract_video_data(video_ids):
    extracted_data = []

    def batch_list(video_id_lst, batch_size):
        for video_id in range(0, len(video_id_lst), batch_size):
            yield video_id_lst[video_id:video_id + batch_size]
    try:
        for batch in batch_list(video_ids, MAX_RESULTS):
            video_ids_str = ",".join(batch)

            url = f"https://youtube.googleapis.com/youtube/v3/videos?part=contentDetails&part=snippet&part=statistics&id={video_ids_str}&key={API_KEY}"
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            for item in data.get("items", []):
                video_data = {
                    "video_id": item["id"],
                    "title": item["snippet"]["title"],
                    "published_at": item["snippet"]["publishedAt"],
                    "duration": item["contentDetails"]["duration"],
                    "view_count": item["statistics"].get("viewCount", None),
                    "like_count": item["statistics"].get("likeCount", None),
                    "comment_count": item["statistics"].get("commentCount", None)
                }
                extracted_data.append(video_data)
        return extracted_data
    except requests.exceptions.RequestException as e:
        raise e

@task
def save_to_json(extracted_data):
    file_path = f"./data/youtube_data_{date.today()}.json"
    try:
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(extracted_data, f, indent=4, ensure_ascii=False)
    except IOError as e:
        raise e


if __name__ == "__main__":
    playlist_id = get_playlist_id()
    video_ids = get_video_ids(playlist_id)
    video_data = extract_video_data(video_ids)
    save_to_json(video_data)
