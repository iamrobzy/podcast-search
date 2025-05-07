import os
import json
from openai import OpenAI
import numpy as np
from enum import IntEnum, Enum
import time
from tqdm import tqdm
import logging
from pydantic import BaseModel, Field

from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(level=logging.INFO)

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

PROMPT = """

Perfect (4): this grade is used only for “known item” and “refinding” topic types. It reflects the segment that is the earliest entry point into the one episode that the user is seeking.
Excellent (3): the segment conveys highly relevant information, is an ideal entry point for a human listener, and is fully on topic. An example would be a segment that begins at or very close to the start of a discussion on the topic, immediately signalling relevance and context to the user.
Good (2): the segment conveys highly-to-somewhat relevant information, is a good entry point for a human listener, and is fully to mostly on topic. An example would be a segment that is a few minutes “off” in terms of position, so that while it is relevant to the user’s information need, they might have preferred to start two minutes earlier or later.
Fair (1): the segment conveys somewhat relevant information, but is a sub-par entry point for a human listener and may not be fully on topic. Examples would be segments that switch from non-relevant to relevant (so that the listener is not able to immediately understand the relevance of the segment), segments that start well into a discussion without providing enough context for understanding, etc.
Bad (0): the segment is not relevant.

"""

class Rating(str, Enum):
  EXCELLENT = "3"
  GOOD = "2"
  FAIR = "1"
  BAD = "0"

class Review(BaseModel):
    rating: Rating

def evaluate_segment(query, segment):

    prompt = """
        Rank the relevance of the following segment for the given query between 1 and 5 according to the following PEGFB graded scale (Perfect, Excellent, Good, Fair, Bad)

        Query: {query}
        
        Segment: {segment}

        Scale: {PROMPT}

        Return a number between 0 and 4 inclusive.
    """

    response = client.responses.parse(
        model="gpt-4o-mini",
        input=[
            {"role": "system", "content": "You are a podcast clip evaluator. You will be given a query and a clip of text from a podcast. You will rate the relevance of the clip for the query on a scale of 0 to 4."},
            {
                "role": "user",
                "content": prompt,
            },
        ],
        text_format=Review,
    )
    rating = int(response.output_parsed.rating)
    return rating


def get_ndcg(rankings):
    idcg = dcg(sorted(rankings, reverse=True))
    if idcg == 0:
        return 0
    return dcg(rankings) / idcg

def dcg(rankings):
    dcg = 0.0
    for i, r in enumerate(rankings):
        dcg += (2 ** r - 1) / np.log2(i + 2)
    return dcg


def get_rating(query, results, K=5, sleep_time=2, verbose=False):
    
    ratings = []
    for segment in tqdm(results[:K]):
        rating = evaluate_segment(query, segment)
        ratings.append(rating)
        time.sleep(sleep_time) # Rate limiting
    
    if verbose:
        for i, rating in enumerate(ratings):
            logging.info(f"Segment {i}: {rating}")

    return ratings


def save_query_results(query):

    from query import query_index

    options = ("bm25", "LM Dirichlet", "IB", "RRF")

    INDEX_NAME_MAP = {
    "bm25": "podcast_test",
    "LM Dirichlet": "podcast_test_lm",
    "IB": "podcast_test_ib"
    }

    clip_length = 2

    # Increment folder number

    i = 0
    while os.path.exists(os.path.join(EVAL_DIR, str(i))):
        i += 1
    new_dir = os.path.join(EVAL_DIR, str(i))
    os.mkdir(new_dir)

    # Evaluate each index

    for option in options:
        
        results = query_index(query, option, clip_length_min=clip_length)

        eval_data = {
            "query": query,
            "clip_length": clip_length,
            "selected_index": option,
            "results": results
        }
        

        with open(os.path.join(new_dir, f"{option}_{clip_length}.json"), "w") as f:
            json.dump(eval_data, f)

    

if __name__ == "__main__":

    EVAL_DIR = "./evals"

    ### Save search results to disk

    # with open("queries.txt") as f:
    #     for query in f:
    #         query = query.strip()
    #         save_query_results(query)
    # quit()

    ### Evaluate results with LLM

    # Iterates through queries

    for folder in os.listdir(EVAL_DIR):

        logging.info(f"Processing folder {folder}")
        eval = {"nDCG": {}}

        # Skip if already evaluated
        if os.path.exists(os.path.join(EVAL_DIR, folder, "eval.json")):
            continue
        
        # Iterates through index types
        for file in tqdm(os.listdir(os.path.join(EVAL_DIR, folder))):

            if file == "eval.json":
                continue

            with open(os.path.join(EVAL_DIR, folder, file), "r") as f:
                data = json.load(f)

            query, clip_length, selected_index, results = list(data.values())

            rating = get_rating(query, results, K=10, sleep_time=0, verbose=False)
            ndcg = get_ndcg(rating)

            eval["nDCG"][selected_index] = {"ndcg": ndcg, "ratings": rating}

            logging.info(f"nDCG for {selected_index}: {ndcg}")

        with open(os.path.join(EVAL_DIR, folder, "eval.json"), "w") as f:
            json.dump(eval, f)

        logging.info(eval)
        
