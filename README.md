# Podcast Search

### Getting started

1. Install dependenices. (We use the package manager *uv*.)

```
uv init
source .venv/bin/activate
uv add -r requirements.txt
```

2. Run the [Elastic Search Engine](https://github.com/elastic/elasticsearch) is running locally on http://localhost:9200.

3. Parse the indices by running `python json_parsing.py`.

4. Spin up the front end by running `streamlit run query.py`. Open http://localhost:2000 in your browser.

5. Enter your search query and select the clip length, then click Start Search