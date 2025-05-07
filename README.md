# Podcast Search

### Getting started

1. Clone the repository.

```bash
git clone https://github.com/iamrobzy/podcast-search.git
```

2. Download and unzip the Spotify transcripts test dataset (shared individually from Johan Boye via KTH One Drive).

3. Initialize virtual environment and install dependenices. (We use the package manager *uv*.)

```
uv init
source .venv/bin/activate
uv add -r requirements.txt
```

4. Set up the environment variables from the [Google Gemini API](https://aistudio.google.com/apikey). Copy the `.env.example` file to `.env` and fill in the values.

5. Run the [Elastic Search Engine](https://github.com/elastic/elasticsearch) is running locally on http://localhost:9200.

6. Parse the indices by running `python json_parsing.py`.

7. Spin up the front end by running `streamlit run query.py`. Open http://localhost:2000 in your browser.

8. Enter your search query and select the clip length, then click Start Search