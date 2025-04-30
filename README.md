# podcast-search
# Run the front end query.py
1. First install dependencies streamlit, elasticsearch and pandas.

2. Make sure the Elastic Search Engine is running locally on http://localhost:9200.

3. Modify the value of the index name in the first line with your index name (like podcast_test)

4. Then run the command 
```
streamlit run app.py
```

5. Enter the key word you want to query and select the clip length, then click Start Search