# IMDb Movie Data Extraction & Scraping
This project extracts and enriches IMDb movie data (2018–2019) by combining web scraping 
with official IMDb datasets.
The result is a structured dataset containing movie metadata, financial data, ratings, genres,
and popularity metrics.

## Features
- Scrapes IMDb movie titles and links using Selenium
- Extracts genres from the official IMDb dataset (title.basics.tsv)
- Scrapes movie details (budget, worldwide gross, runtime, ratings and vote counts)
- Computes popularity scores for actors and directors

   For each actor/director:

  - The “Known For” section on the actor’s IMDb page is scraped

  - Star ratings of the listed movies are extracted

  - The actor/director’s popularity score is computed as the mean of these ratings

  - This score is used as a proxy for actor popularity and cached locally to avoid repeated scraping.
  
- Uses SQLite for caching and resumable scraping
- Exports the final dataset to CSV

## Project Structure
```bash
.
├── TitleLinkExtraction.py      # Scrape IMDb movie titles and links
├── extract_genres.py           # Extract genres using IMDb datasets
├── InfoMoviesExtraction.py    # Scrape detailed movie information
├── data/                       # IMDb datasets (not tracked by git)
├── movies.db                  # SQLite cache (auto-generated)
├── *.csv                      # Generated outputs
└── README.md

```

## IMDb Datasets (Required)
This project depends on official IMDb datasets, which **must be downloaded manually**.

Download from:
[IMDb public datasets](https://datasets.imdbws.com/)

Required files:

- `title.basics.tsv.gz`

Extract them into a local `data/` directory:
```bash
mkdir data
gunzip title.basics.tsv.gz
mv title.basics.tsv data/

```
## How to Run 
**1. Scrape movie titles and links**
```bash
python TitleLinkExtraction.py

```
**2. Extract genres**
```bash
python extract_genres.py


```
**3. Scrape detailed movie data**
```bash
python InfoMoviesExtraction.py

```

The final output will be saved as:
```bash
movies_data.csv
```


