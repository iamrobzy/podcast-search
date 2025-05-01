index_name="podcast_test"
import streamlit as st
from elasticsearch import Elasticsearch
import pandas as pd
import requests


es = Elasticsearch("http://localhost:9200")

st.title("Podcast Clip Search")

query = st.text_input("Enter a search keyword")

clip_length_min = st.slider("Select total clip length (minutes)", 1, 10, 2)

# Search button
if st.button("Start Search") and query.strip():
    body = {
        "query": {
            "match": {
                "word_list": query
            }
        },
        "highlight": {
            "fields": {
                "word_list": {}
            },
            "pre_tags": ["<mark>"],
            "post_tags": ["</mark>"]
        }
    }

    #res = es.search(index=index_name, body=body, size=50)
    res = requests.get("http://127.0.0.1:9200/podcast_test/_search", json=body, auth=("elastic", "ON9oupZ1"))
    
    def extract_clip_fixed_length(word_list, time_start_list, time_end_list, target_words, clip_seconds=60):
        clips = []
        target_words_lower = [word.lower() for word in target_words]
        half_clip = clip_seconds / 2
    
        visited_end = 0
        #Indicies of words that are in our query
        target_indices = [i for i, word in enumerate(word_list) if word.lower() in target_words_lower]

        for idx in target_indices:
            center_start = time_start_list[idx]
            
            #We've already covered around this word in another clip so we dont need to do it again
            if center_start < visited_end - 10:
                continue
            window_start = center_start - half_clip
            window_end = center_start + half_clip

            visited_end = window_end

            clip_words = []
            clip_start_time = None
            clip_end_time = None

            for i in range(len(word_list)):
                if (time_start_list[i] >= window_start) and (time_end_list[i] <= window_end):
                    word = word_list[i]
                    if word.lower() in [w.lower() for w in target_words]:
                        word = f"<mark>{word}</mark>"
                    clip_words.append(word)
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

    for hit in res.json()["hits"]["hits"]:
        source = hit["_source"]
        highlights = hit.get("highlight", {}).get("word_list", [])
        word_list = source.get("word_list", [])
        time_start_list = source.get("time_start", [])
        time_end_list = source.get("time_end", [])

        if not word_list or not time_start_list or not time_end_list:
            continue

        target_words = query.strip().split()

        clips = extract_clip_fixed_length(
            word_list, time_start_list, time_end_list,
            target_words, clip_seconds=clip_length_min * 60
        )

        for clip in clips:
            clip["show_uri"] = source.get("show_uri", "")
            clip["show_name"] = source.get("show_name", "")
            clip["show_description"] = source.get("show_description", "")
            clip["publisher"] = source.get("publisher", "")
            clip["episode_uri"] = source.get("episode_uri", "")
            clip["episode_name"] = source.get("episode_name", "")
            clip["episode_description"] = source.get("episode_description", "")
            clip["score"] = hit["_score"]
            final_results.append(clip)

    if final_results:
        df = pd.DataFrame(final_results)
        df.sort_values(by=["score"], ascending=False)
        st.markdown("### Search Results")
        for _, row in df.iterrows():
            st.markdown(f"""
                <div style='padding:10px; margin-bottom:15px; background:#f9f9f9; border-left: 4px solid #ccc'>
                    <b>Clip:</b> {row['Clip Text']}<br>
                    <b>Start:</b> {row['Start Time (s)']}s | <b>End:</b> {row['End Time (s)']}s<br>
                    <b>Episode:</b> {row['episode_name']}<br>
                    <b>Show:</b> {row['show_name']}<br>
                    <b>Publisher:</b> {row['publisher']}<br>
                    <b>Score:</b> {row['score']}<br>
                </div>
            """, unsafe_allow_html=True)
    else:
        st.warning("No matching clips found. Please try another keyword.")
