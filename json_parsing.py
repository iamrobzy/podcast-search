import json
import os
import time
import requests
DIR_PATH = "./podcasts-no-audio-13GB/spotify-podcasts-2020-summarization-testset/spotify-podcasts-2020/"
PASSWORD = "ON9oupZ1"
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
                startTimes.append(float(word["startTime"][0:-1]))
                endTimes.append(float(word["endTime"][0:-1]))
    string = " ".join(word_list)
    return {"text": string, "word_list": word_list, "time_start": startTimes, "time_end": endTimes}

def process_metadata(metadata):
    parts = metadata.split("	")
    res = {}
    res["show_uri"] = parts[0]
    res["show_name"] = parts[1]
    res["show_description"] = parts[2]
    res["publisher"] = parts[3]
    res["language"] = parts[4]
    res["rss_link"] = parts[5]
    res["episode_uri"] = parts[6]
    res["episode_name"] = parts[7]
    res["episode_description"] = parts[8]
    res["duration"] = parts[9]
    res["show_filename_prefix"] = parts[10]
    res["episode_filename_prefix"] = parts[11]
    return res
    
def sort_metadata():
    res = []

    with open(DIR_PATH + "metadata-summarization-testset.tsv") as f:
    #with open("./podcasts-no-audio-13GB/metadata.tsv") as f:
        i = 1
        lines = []
        line = f.readline()
        lines.append(line)

        line = f.readline()
        while line:
            lines.append(line)
            parts = line.split("	")
            res.append([parts[0] + parts[6], i])
            line = f.readline()
            i += 1
    def sorting_func(item):
        return item[0].upper()
    res.sort(key=sorting_func)

    with open("metadata_sorted.tsv", "w") as f:
        for item in res:
            i = item[1]
            f.write(lines[i])

def insert_into_index(json: dict, id = 1):
    response = requests.post("http://127.0.0.1:9200/podcast_test/_doc/" + str(id), json=json, auth=("elastic", PASSWORD))
    #print("response: ", response)
    return response

print("start metadata sorting")       
start = time.time()
if not os.path.exists("metadata_sorted.txt"):
    sort_metadata()

print("metadata sorting done in" , time.time() - start, "seconds")



path = DIR_PATH + "podcasts-transcripts-summarization-testset/" 
dirs = os.listdir(path)
dirs.sort()


meta_file = open("metadata_sorted.tsv", "r")
processed_files = 0
print("start json processing")  
start = time.time()
def upper_sorter(item):
    return item.upper()
for dir in dirs: #For all numbers (0,1,...,8)
    path_inner = path + dir + "/"
    dirs_inner = os.listdir(path_inner)
    dirs_inner.sort(key=upper_sorter)
    for dir_inner in dirs_inner: #For all chars 0,1,...,9, A, B, ... Z
        path_inner_inner = path_inner + dir_inner + "/"
        shows = os.listdir(path_inner_inner)
        shows.sort(key=upper_sorter)
        for show in shows: #For all podcast episodes 
            show_path = path_inner_inner + show + "/"
            episodes = os.listdir(show_path)
            episodes.sort(key=upper_sorter)
            for episode in episodes:
                episode_path = show_path + episode + ""
                
                with open(episode_path, "r") as f:
                    res = process_json_file(json.load(f))

                metadata = meta_file.readline()              
                meta = process_metadata(metadata)
                #print("found episode: \n" + episode_path[138:])
                #print(meta["show_uri"][8:] + "/" +meta["episode_filename_prefix"])
                for key in res:
                    meta[key] = res[key]
                processed_files += 1

                insert_into_index(meta, processed_files)
                
                if (processed_files % 100 == 0):
                    print("processed", processed_files, "files")
meta_file.close()

print("json parsing done in" , time.time() - start, "seconds")

#Run test query
query = {
  "query" : {
    "match" : { "show_name": "IrishIllustrated.com Insider" }
  }
}
response = requests.get("http://127.0.0.1:9200/podcast_test/_search", json=query, auth=("elastic", "ON9oupZ1"))

print("got response:" , response.content)

