import json
import os
import requests

DIR_PATH = "D://podcasts-no-audio-13GB/spotify-podcasts-2020-summarization-testset/spotify-podcasts-2020/"
PASSWORD = "ON9oupZ1"
INDEX_NAME = "podcast_test_ib"
ES_URL = f"http://127.0.0.1:9200/{INDEX_NAME}"

def process_json_file(data):
    res = data["results"]
    word_list = []
    startTimes = []
    endTimes = []
    for part in res:
        alt = part["alternatives"][0]
        if "transcript" in alt:
            words = alt["words"]
            for word in words:
                word_list.append(word["word"])
                startTimes.append(float(word["startTime"][:-1]))
                endTimes.append(float(word["endTime"][:-1]))
    string = " ".join(word_list)
    return {
        "text": string,
        "word_list": word_list,
        "time_start": startTimes,
        "time_end": endTimes
    }

def process_metadata(metadata):
    parts = metadata.split("\t")
    return {
        "show_uri": parts[0],
        "show_name": parts[1],
        "show_description": parts[2],
        "publisher": parts[3],
        "language": parts[4],
        "rss_link": parts[5],
        "episode_uri": parts[6],
        "episode_name": parts[7],
        "episode_description": parts[8],
        "duration": float(parts[9]) if parts[9] else 0.0,
        "show_filename_prefix": parts[10],
        "episode_filename_prefix": parts[11]
    }

def insert_into_index(json_doc: dict, doc_id=1):
    response = requests.post(f"{ES_URL}/_doc/{doc_id}", json=json_doc, auth=("elastic", PASSWORD))
    return response

def delete_index():
    response = requests.delete(ES_URL, auth=("elastic", PASSWORD))
    print("Delete index response:", response.status_code, response.text)

def create_index():
    index_config = {
        "settings": {
            "similarity": {
                "my_ib": {
                    "type": "IB",
                    "distribution": "ll",
                    "lambda": "df",
                    "normalization": "z"
                }
            }
        },
        "mappings": {
            "properties": {
                "episode_uri": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}},
                "show_description": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}},
                "time_start": {"type": "float"},
                "show_name": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}},
                "language": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}},
                "episode_description": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}},
                "duration": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}},
                "show_filename_prefix": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}},
                "episode_name": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}},
                "episode_filename_prefix": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}},
                "publisher": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}},
                "show_uri": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}},
                "word_list": {
                    "type": "text",
                    "similarity": "my_ib",
                    "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}
                },
                "rss_link": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}},
                "text": {
                    "type": "text",
                    "similarity": "my_ib",
                    "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}
                },
                "time_end": {"type": "float"}
            }
        }
    }
    response = requests.put(ES_URL, headers={"Content-Type": "application/json"},
                            data=json.dumps(index_config), auth=("elastic", PASSWORD))
    print("Create index response:", response.status_code, response.text)

# Reset index
delete_index()
create_index()

# Load and insert data
meta_file = open("metadata_sorted.tsv", "r", encoding='utf-8')
path = os.path.join(DIR_PATH, "podcasts-transcripts-summarization-testset")
dirs = sorted(os.listdir(path))
processed_files = 0

for dir in dirs:
    path_inner = os.path.join(path, dir)
    dirs_inner = sorted(os.listdir(path_inner), key=lambda x: x.upper())
    for dir_inner in dirs_inner:
        path_inner_inner = os.path.join(path_inner, dir_inner)
        shows = sorted(os.listdir(path_inner_inner), key=lambda x: x.upper())
        for show in shows:
            show_path = os.path.join(path_inner_inner, show)
            episodes = sorted(os.listdir(show_path), key=lambda x: x.upper())
            for episode in episodes:
                episode_path = os.path.join(show_path, episode)
                with open(episode_path, "r", encoding='utf-8') as f:
                    res = process_json_file(json.load(f))
                metadata = meta_file.readline()
                meta = process_metadata(metadata)

                # Unify word_list to string format
                meta["text"] = res["text"]
                meta["word_list"] = " ".join(res["word_list"])
                meta["time_start"] = res["time_start"]
                meta["time_end"] = res["time_end"]

                processed_files += 1
                insert_into_index(meta, processed_files)
                if processed_files % 100 == 0:
                    print("Processed", processed_files, "files")

meta_file.close()

# Sample search query
query = {
    "query": {
        "match": {
            "show_name": "IrishIllustrated.com Insider"
        }
    }
}

response = requests.get(f"{ES_URL}/_search", json=query, auth=("elastic", PASSWORD))
data = response.json()

with open("output.json", "w", encoding="utf-8") as f:
    json.dump(data, f, ensure_ascii=False, indent=2)

print("Search results written to output.json")
