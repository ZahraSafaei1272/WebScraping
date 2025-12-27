# IMDb Movie Data Extraction & Scraping
This project extracts and enriches IMDb movie data (2018–2019) by combining web scraping 
with official IMDb datasets.
The result is a structured dataset containing movie metadata, financial data, ratings, genres,
and popularity metrics.

## Features
**Scrapes IMDb movie titles and links using Selenium**
**Extracts genres from the official IMDb dataset (title.basics.tsv)**
**Scrapes movie details (budget, worldwide gross, runtime)**
**Computes popularity scores for actors and directors**
**Uses SQLite for caching and resumable scraping**
**Exports the final dataset to CSV**
