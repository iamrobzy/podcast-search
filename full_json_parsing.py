import json
import os
import time
import requests
DIR_PATH = "../podcasts-no-audio-13GB/"
PASSWORD = "ON9oupZ1"
ES_URL = "http://127.0.0.1:9200/"

do_process_metadata = True
do_populate_index = {
    "bm25": True,
    "LM Dirichlet": True,
    "IB": True,
}
do_run_test_query = True

index_name_map = {
    "bm25": "podcast_bm25",
    "LM Dirichlet": "podcast_lm",
    "IB": "podcast_ib"
}
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

    with open(DIR_PATH + "metadata.tsv",encoding='utf-8') as f:
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

    with open("full_metadata_sorted.tsv", "w",encoding='utf-8') as f:
        for item in res:
            i = item[1]
            f.write(lines[i])

def insert_into_index(json: dict, target_index_path, id = 1):
    response = requests.post("http://127.0.0.1:9200/" + target_index_path + "/_doc/" + str(id), json=json, auth=("elastic", PASSWORD))
    #print("response: ", response)
    return response

def insert_bulk(bulk_data, target_index_path):
    #bulk_data += "\n"
    #request = requests.request(method = "PUT", url="http://127.0.0.1:9200/", data={"index": bulk_data})
    #request.headers["Authorization"] = ("elastic", PASSWORD)
    #request.headers["Content-Type"] = "application/json"
    #response = requests.put(request)
    response = requests.put("http://127.0.0.1:9200/" + target_index_path + "/_bulk", data=bulk_data, auth=("elastic", PASSWORD), headers={"Content-Type": "application/json"})
    #print("response: ", response)
    #print(response.text)
    return response

def delete_index(index_name):
    index_path = index_name_map[index_name]

    response = requests.delete(ES_URL + index_path, auth=("elastic", PASSWORD))
    print("Delete index response:", response.status_code, response.text)

def create_index(index_name):
    index_path = index_name_map[index_name]
    if index_name == "bm25":
        index_config = {}
    if index_name == "LM Dirichlet":
        index_config = {
            "settings": {
                "similarity": {
                    "my_lm": {
                        "type": "LMDirichlet",
                        "mu": 2000
                    }
                }
            },
            "mappings": {
                "properties": {
                    "episode_uri": {
                        "type": "text",
                        "fields": {
                            "keyword": {
                                "type": "keyword",
                                "ignore_above": 256
                            }
                        }
                    },
                    "show_description": {
                        "type": "text",
                        "fields": {
                            "keyword": {
                                "type": "keyword",
                                "ignore_above": 256
                            }
                        }
                    },
                    "time_start": {
                        "type": "float"
                    },
                    "show_name": {
                        "type": "text",
                        "fields": {
                            "keyword": {
                                "type": "keyword",
                                "ignore_above": 256
                            }
                        }
                    },
                    "language": {
                        "type": "text",
                        "fields": {
                            "keyword": {
                                "type": "keyword",
                                "ignore_above": 256
                            }
                        }
                    },
                    "episode_description": {
                        "type": "text",
                        "fields": {
                            "keyword": {
                                "type": "keyword",
                                "ignore_above": 256
                            }
                        }
                    },
                    "duration": {
                        "type": "text",
                        "fields": {
                            "keyword": {
                                "type": "keyword",
                                "ignore_above": 256
                            }
                        }
                    },
                    "show_filename_prefix": {
                        "type": "text",
                        "fields": {
                            "keyword": {
                                "type": "keyword",
                                "ignore_above": 256
                            }
                        }
                    },
                    "episode_name": {
                        "type": "text",
                        "fields": {
                            "keyword": {
                                "type": "keyword",
                                "ignore_above": 256
                            }
                        }
                    },
                    "episode_filename_prefix": {
                        "type": "text",
                        "fields": {
                            "keyword": {
                                "type": "keyword",
                                "ignore_above": 256
                            }
                        }
                    },
                    "publisher": {
                        "type": "text",
                        "fields": {
                            "keyword": {
                                "type": "keyword",
                                "ignore_above": 256
                            }
                        }
                    },
                    "show_uri": {
                        "type": "text",
                        "fields": {
                            "keyword": {
                                "type": "keyword",
                                "ignore_above": 256
                            }
                        }
                    },
                    "word_list": {
                        "type": "text",
                        "similarity": "my_lm",
                        "fields": {
                            "keyword": {
                                "type": "keyword",
                                "ignore_above": 256
                            }
                        }
                    },
                    "rss_link": {
                        "type": "text",
                        "fields": {
                            "keyword": {
                                "type": "keyword",
                                "ignore_above": 256
                            }
                        }
                    },
                    "text": {
                        "type": "text",
                        "similarity": "my_lm",
                        "fields": {
                            "keyword": {
                                "type": "keyword",
                                "ignore_above": 256
                            }
                        }
                    },
                    "time_end": {
                        "type": "float"
                    }
                }
            }
        }
    elif index_name == "IB":
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
    response = requests.put(ES_URL + index_path, headers={"Content-Type": "application/json"}, data=json.dumps(index_config), auth=("elastic", PASSWORD))
    print("Create index response:", response.status_code, response.text)


def parse_json(target_index, indexes_to_populate):
    target_index_path = index_name_map[target_index]
    dirs = []
    parts = ["podcasts-transcripts-0to2/", "podcasts-transcripts-3to5/", "podcasts-transcripts-6to7/"]

    print("start json processing")  
    start = time.time()
    for part in parts:
        part = part + "spotify-podcasts-2020/podcasts-transcripts/"
        path = DIR_PATH + part
        res = os.listdir(path)
        for r in res:
            dirs.append(part + r + "/")

    def upper_sorter(item):
        return item.upper()

    dirs.sort(key=upper_sorter)
    print(dirs)

    meta_file = open("full_metadata_sorted.tsv", "r",encoding='utf-8')
    processed_files = 0

    bulk_data = ""
    
    for dir in dirs: #For all numbers (0,1,...,8)
        path_inner = DIR_PATH + dir
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
                    
                    with open(episode_path, "r",encoding='utf-8') as f:
                        res = process_json_file(json.load(f))

                    metadata = meta_file.readline()              
                    meta = process_metadata(metadata)
                    #print("found episode: \n" + episode_path[127:])
                    #print(meta["show_uri"][8:] + "/" +meta["episode_filename_prefix"])
                    for key in res:
                        meta[key] = res[key]
                    processed_files += 1
                    bulk_data += "{\"create\": {}}\n"
                    bulk_data += json.dumps(meta) + "\n"
                    #for key in indexes_to_populate:
                    #    if indexes_to_populate[key]:
                    #        insert_into_index(meta, index_name_map[key], processed_files)

                    
                    if (processed_files % 100 == 0):
                        print("processed", processed_files, "files")
                        
                    if (processed_files % 500 == 0):
                        for key in indexes_to_populate:
                            if indexes_to_populate[key]:
                                #insert_into_index(meta, index_name_map[key], processed_files)
                                insert_bulk(bulk_data, index_name_map[key])
                        bulk_data = ""
                        print("elapsed time: ", time.time() - start)
    meta_file.close()

    print("json parsing done in" , time.time() - start, "seconds")

if do_process_metadata:
    print("start metadata sorting")       
    start = time.time()
    sort_metadata()
    print("metadata sorting done in" , time.time() - start, "seconds")

for index_name in do_populate_index:
    if do_populate_index[index_name]:
        delete_index(index_name)
        create_index(index_name)

parse_json(index_name, do_populate_index)
#Run test query
if do_run_test_query:
    query = {
    "query" : {
        "match" : { "show_name": "IrishIllustrated.com Insider" }
    }
    }
    response = requests.get("http://127.0.0.1:9200/podcast_test/_search", json=query, auth=("elastic", "ON9oupZ1"))

    # print("got response:" , response.content)
    data = response.json()
    with open("output.json", "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)