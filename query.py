index_name="podcast_test"
import streamlit as st
from elasticsearch import Elasticsearch
import pandas as pd
import requests
import json
from dotenv import load_dotenv
load_dotenv(dotenv_path="../elasticsearch/elastic-start-local/.env")
import os

PASSWORD = os.getenv("ES_LOCAL_PASSWORD")
ES_URL = os.getenv("ES_LOCAL_URL")

es = Elasticsearch(ES_URL)

### UI components

st.title("Podcast Clip Search") # Title
query = st.text_input("Enter a search keyword") # Query

selected_index = st.selectbox(
    "Select search method",
    ("bm25", "LM Dirichlet", "IB", "RRF"),
)

index_name_map = {
    "bm25": "podcast_test",
    "LM Dirichlet": "podcast_test_lm",
    "IB": "podcast_test_ib"
}

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
        },
        "explain": True
    }

    if selected_index == "RRF":
        from collections import defaultdict
        k = 60
        rrf_scores = defaultdict(float)
        hits_by_id = {}
        for method, idx in index_name_map.items():
            response = requests.get(os.path.join(ES_URL, idx, "_search"), json=body, auth=("elastic", PASSWORD))
            print(f"{method} returned", response.status_code)
            hits = response.json()["hits"]["hits"]
            for rank, hit in enumerate(hits):
                docid = hit["_id"]
                rrf_scores[docid] += 1.0/(k + rank + 1)
                if docid not in hits_by_id:
                    hits_by_id[docid] = hit
        fused = sorted(rrf_scores.items(), key=lambda x: x[1], reverse=True)
        fused_hits = [hits_by_id[docid] for docid,_ in fused]
    else:
        target_index = index_name_map[selected_index]
        res = requests.get(os.path.join(ES_URL, target_index, "_search"), json=body, auth=("elastic", PASSWORD))
        print("response status code: ", res.status_code)
        fused_hits = res.json()["hits"]["hits"]

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
                    if word.lower() in target_words_lower:
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

    #for hit in res.body["hits"]["hits"]
    # for hit in res.json()["hits"]["hits"]:
    for hit in fused_hits:
        source = hit["_source"]
        explanation = hit.get("_explanation", {})
        explanation_text = json.dumps(explanation, indent=2)

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
            clip["explanation"] = explanation_text
            final_results.append(clip)

    if final_results:
        if True:
            grouped = {}
            for clip in final_results:
                ep_name = clip.get("episode_name", "Unknown Episode")
                grouped.setdefault(ep_name, []).append(clip)

            for ep_name, clips in grouped.items():
                with st.expander(f"📻 {ep_name}"):
                    for clip in clips:
                        st.markdown(f"""
                            <div style='padding:10px; margin-bottom:10px; background:#f9f9f9; border-left: 4px solid #ccc'>
                                <b>Clip:</b> {clip['Clip Text']}<br>
                                <b>Start:</b> {clip['Start Time (s)']}s | <b>End:</b> {clip['End Time (s)']}s<br>
                                <b>Show:</b> {clip['show_name']}<br>
                                <b>Publisher:</b> {clip['publisher']}<br>
                                <b>Score:</b> {clip['score']}<br>
                                <details>
                                    <summary><b>Explanation (Click to Expand)</b></summary>
                                    <pre>{clip['explanation']}</pre>
                                </details>
                            </div>
                        """, unsafe_allow_html=True)
    else:
        st.warning("No matching clips found. Please try another keyword.")
