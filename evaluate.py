import os
import json
from google import genai
import numpy as np
from enum import IntEnum, Enum
import time
from tqdm import tqdm

from dotenv import load_dotenv
load_dotenv() 

client = genai.Client(api_key=os.getenv("GOOGLE_API_KEY"))

PROMPT = """

Perfect (4): this grade is used only for “known item” and “refinding” topic types. It reflects the segment that is the earliest entry point into the one episode that the user is seeking.
Excellent (3): the segment conveys highly relevant information, is an ideal entry point for a human listener, and is fully on topic. An example would be a segment that begins at or very close to the start of a discussion on the topic, immediately signalling relevance and context to the user.
Good (2): the segment conveys highly-to-somewhat relevant information, is a good entry point for a human listener, and is fully to mostly on topic. An example would be a segment that is a few minutes “off” in terms of position, so that while it is relevant to the user’s information need, they might have preferred to start two minutes earlier or later.
Fair (1): the segment conveys somewhat relevant information, but is a sub-par entry point for a human listener and may not be fully on topic. Examples would be segments that switch from non-relevant to relevant (so that the listener is not able to immediately understand the relevance of the segment), segments that start well into a discussion without providing enough context for understanding, etc.
Bad (0): the segment is not relevant.

"""

class Rating(Enum):
  EXCELLENT = "3"   
  GOOD = "2"
  FAIR = "1"
  BAD = "0"  


def evaluate_segment(query, segment):

    response = client.models.generate_content(
        model='gemini-2.0-flash',
        contents=
        """
        Rank the relevance of the following segment for the given query between 1 and 5 according to the following PEGFB graded scale (Perfect, Excellent, Good, Fair, Bad)

        Query: {query}
        
        Segment: {segment}

        Scale: {PROMPT}

        Return a number between 0 and 4 inclusive.
        """,
        config={
            'response_mime_type': 'application/json',
            'response_schema': Rating,
        },
    )
    # Use the response as a JSON string.
    return response.parsed


def ndcg(rankings):
    idcg = dcg(sorted(rankings, reverse=True))
    if idcg == 0:
        return 0
    return dcg(rankings) / idcg

def dcg(rankings):
    dcg = 0.0
    for i, r in enumerate(rankings):
        dcg += (2 ** r - 1) / np.log2(i + 2)
    return dcg


def evaluate(query, results, N=5):
    
    ratings = []
    for segment in tqdm(results[:N]):
        rating = int(evaluate_segment(query, segment).value)
        ratings.append(rating)
    time.sleep(2) # Rate limiting
    return ndcg(ratings)

if __name__ == "__main__":

    EVAL_DIR = "./evals"

    for file in os.listdir(EVAL_DIR):
        with open(os.path.join(EVAL_DIR, file), "r") as f:
            data = json.load(f)

    query, clip_length, selected_index, results = data.values()

    rating = evaluate(query, results)
    print(rating)
