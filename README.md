# Podcast Search

### Getting started

1. Install dependenices. (We use the package manager *uv*.)

```
uv init
source .venv/bin/activate
uv add -r requirements.txt
```

2. Set up the environment variables. Copy the `.env.example` file to `.env` and fill in the values.

3. Run the [Elastic Search Engine](https://github.com/elastic/elasticsearch) is running locally on http://localhost:9200.

4. Parse the indices by running `python json_parsing.py`.

5. Spin up the front end by running `streamlit run query.py`. Open http://localhost:2000 in your browser.

6. Enter your search query and select the clip length, then click Start Search