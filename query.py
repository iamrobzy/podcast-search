import streamlit as st
from elasticsearch import Elasticsearch
import pandas as pd

# Connect to Elasticsearch
es = Elasticsearch(
    "http://localhost:9200",
)

st.title("Podcast Clip Search")

# Input search keyword
query = st.text_input("Enter a search keyword")

# User selects total clip length (in minutes)
clip_length_min = st.slider("Select total clip length (minutes)", 1, 10, 2)

# Search button
if st.button("Start Search"):
    # Elasticsearch query
    body = {
        "query": {
            "match": {
                "word_list": query
            }
        }
    }

    res = es.search(index="podcast_test", body=body, size=50)

    # Helper function: extract clip with fixed total length
    def extract_clip_fixed_length(word_list, time_start_list, time_end_list, target_words, clip_seconds=60):
        clips = []

        half_clip = clip_seconds / 2  # Half of the clip, e.g., 30 seconds

        for target_word in target_words:
            target_indices = [i for i, word in enumerate(word_list) if word.lower() == target_word.lower()]
            
            for idx in target_indices:
                center_start = time_start_list[idx]

                window_start = center_start - half_clip
                window_end = center_start + half_clip

                clip_words = []
                clip_start_time = None
                clip_end_time = None

                for i in range(len(word_list)):
                    if (time_start_list[i] >= window_start) and (time_end_list[i] <= window_end):
                        clip_words.append(word_list[i])
                        if clip_start_time is None:
                            clip_start_time = time_start_list[i]
                        clip_end_time = time_end_list[i]

                if clip_words:
                    clips.append({
                        "Clip Text": " ".join(clip_words),
                        "Start Time (s)": clip_start_time,
                        "End Time (s)": clip_end_time,
                        "Clip Length (min)": round((clip_end_time - clip_start_time) / 60, 2)
                    })

        return clips

    final_results = []

    for hit in res.body["hits"]["hits"]:
        source = hit["_source"]
        word_list = source.get("word_list", [])
        time_start_list = source.get("time_start", [])
        time_end_list = source.get("time_end", [])
        
        if not word_list or not time_start_list or not time_end_list:
            continue

        target_words = query.strip().split()

        clips = extract_clip_fixed_length(
            word_list, time_start_list, time_end_list, 
            target_words, clip_seconds=clip_length_min * 60  # Convert to seconds
        )

        for clip in clips:
            clip["show_uri"] = source.get("show_uri", "")
            clip["show_name"] = source.get("show_name", "")
            clip["show_description"] = source.get("show_description", "")
            clip["publisher"] = source.get("publisher", "")
            clip["episode_uri"] = source.get("episode_uri", "")
            clip["episode_name"] = source.get("episode_name", "")
            clip["episode_description"] = source.get("episode_description", "")
            final_results.append(clip)

    if final_results:
        df = pd.DataFrame(final_results)
        st.dataframe(df, use_container_width=True)
    else:
        st.warning("No matching clips found. Please try another keyword.")
