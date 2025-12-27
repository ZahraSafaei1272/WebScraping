import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
import re
from time import sleep
from random import uniform
import statistics

# ======================
# CONFIGURATION
# ======================
DATA_PATH_LINKS = "imdb_titles_links.csv"  # movie_name, link
DATA_PATH_GENRES = "genres.csv"            # movie_name, genre
OUTPUT_FILE = "movies_data.csv"
DB_FILE = "movies.db"

DAILY_BATCH_SIZE = 3000  # Number of movies to process per day

# currency conversion
EXCHANGE_RATES = {
    "$": 1.0,  # USD
    "€": 1.12,  # Average EUR→USD for 2018–2019
    "£": 1.28,  # Average GBP→USD for 2018–2019
}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}
# =========================
# LOAD DATASETS
# =========================
df_links = pd.read_csv(DATA_PATH_LINKS)
df_genres = pd.read_csv(DATA_PATH_GENRES)

# Load IMDb datasets
df_basics = pd.read_csv("title.basics.tsv", sep='\t', dtype=str)
df_ratings = pd.read_csv("title.ratings.tsv", sep='\t', dtype=str)

# Merge data for easy access
df_basics.set_index('tconst', inplace=True)
df_ratings.set_index('tconst', inplace=True)

# =========================
# SQLITE CACHE FUNCTIONS
# =========================
def init_db():
    """
    SQLite cache to store and quickly retrieve popularity scores
    for actors/directors, avoiding repeated web scraping
    """
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("""
            CREATE TABLE IF NOT EXISTS people (
                person_id TEXT PRIMARY KEY,
                person_name TEXT,
                popularity REAL
            )
        """)
    cursor.execute("""
            CREATE TABLE IF NOT EXISTS movie_results (
                movie_name TEXT PRIMARY KEY,
                budget INTEGER,
                gross INTEGER,
                genre TEXT,
                runtime INTEGER,
                rating REAL,
                vote INTEGER,
                pop_actor1 REAL,
                pop_actor2 REAL,
                pop_actor3 REAL,
                pop_director REAL,
                link TEXT
            )
        """)
    conn.commit()
    return conn, cursor

def get_cached_popularity(cursor, person_id):
    cursor.execute("SELECT popularity FROM people WHERE person_id = ?", (person_id,))
    result = cursor.fetchone()
    return result[0] if result else None

def save_popularity(cursor, conn, person_id, person_name, popularity):
    cursor.execute(
        "INSERT OR REPLACE INTO people (person_id, person_name, popularity) VALUES (?, ?, ?)",
        (person_id, person_name, popularity)
    )
    conn.commit()

def get_processed_count(cursor):
    cursor.execute("SELECT COUNT(*) FROM movie_results")
    return cursor.fetchone()[0]

def save_movie_row(cursor, conn, row):
    cursor.execute("""
        INSERT OR REPLACE INTO movie_results
        (movie_name, budget, gross, genre, runtime, rating, vote,
         pop_actor1, pop_actor2, pop_actor3, pop_director, link)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        row['movie_name'], row['budget'], row['gross'], row['genre'],
        row['runtime'], row['rating'], row['vote'],
        row['pop_actor1'], row['pop_actor2'], row['pop_actor3'],
        row['pop_director'], row['link']
    ))
    conn.commit()


# ======================
# HELPER FUNCTIONS
# ======================
def get_imdb_soup(url):
    try:
        # Send HTTP GET request with browser-like headers
        response = requests.get(url, headers=HEADERS, timeout=10)
        # Raise an error if status code is not 200
        response.raise_for_status()
        return BeautifulSoup(response.text, "html.parser")
    except requests.RequestException:
        return None

def extract_number(text):
    """
    Extract numeric value from a string containing currency symbols ($, €, £)
    and convert it to USD using fixed exchange rates for 2018–2019.

    Examples:
        "$10,000,000" -> 10000000
        "€8,000,000" -> 8_960_000
        "£5,000,000" -> 6_400_000
    """
    if not text:
        return None
    try:
        currency = None
        for symbol in EXCHANGE_RATES:
            if symbol in text:
                currency = symbol
                break

        # Remove all non-digit characters except dot or comma
        cleaned = re.sub(r'[^\d.,]', '', text)
        # Remove thousands separator
        cleaned = cleaned.replace(',', '')
        value = float(cleaned)

        # Convert to USD if currency is found
        if currency:
            value *= EXCHANGE_RATES[currency]
        return int(value)
    except ValueError:
        return None

def safe_find_text(soup, tag, attrs, subtag=None, subattrs=None):
    try:
        block = soup.find(tag, attrs)
        if subtag:
            block = block.find(subtag, subattrs)
        return block.text.strip() if block else None
    except AttributeError:
        return None

# ======================
# EXPORT CSV AFTER DONE
# ======================
def export_to_csv():
    conn = sqlite3.connect(DB_FILE)
    df = pd.read_sql_query("SELECT * FROM movie_results", conn)
    df.to_csv(OUTPUT_FILE, index=False)
    conn.close()

# =========================
# SCRAPING FUNCTIONS
# =========================
def get_budget(soup):
    # Extract text from specific IMDb budget field
    text = safe_find_text(soup, 'li', {'data-testid':'title-boxoffice-budget'}, 'li', {'class':'ipc-inline-list__item'})
    return extract_number(text)

def get_gross(soup):
    # Extracts worldwide gross and converts to numeric USD
    text = safe_find_text(soup, 'li', {'data-testid':'title-boxoffice-cumulativeworldwidegross'},
                          'span', {'class':'ipc-metadata-list-item__list-content-item'})
    return extract_number(text)

def get_person_url(soup, selector, index=0):
    """
    Extracts the URL and IMDb person_id (e.g., nm1234567) from a cast/crew element.
    """
    try:
        tag = soup.select(selector)[index]
        href = tag['href']
        if href.startswith("/"):
            return f"https://www.imdb.com{href}", href.split("/")[2]  # person_id
        return href, None
    except (IndexError, TypeError, AttributeError):
        return None, None

def get_popularity_score(person_url):
    """
    Scrapes a person's profile page, extracts star ratings from the "Known For" section,
    and returns their average rating.
    """
    soup = get_imdb_soup(person_url)
    if not soup:
        return None
    section = soup.find("div", {"data-testid":"shoveler-items-container"})
    if not section:
        return None
    ratings = []
    for block in section:
        span = block.find("span", {"class":"ipc-rating-star--rating"})
        if span:
            try:
                value = float(span.text.strip())
                if 0 <= value <= 10:
                    ratings.append(value)
            except ValueError:
                continue
    return round(statistics.mean(ratings),2) if ratings else None

def get_popularity_actor(soup, actor_num, cursor, conn):
    """
     Gets actor popularity; uses cache if available; scrapes IMDb only when missing
    """
    url, person_id = get_person_url(soup,'a[data-testid="title-cast-item__actor"]', actor_num-1)
    if not url or not person_id:
        return None
    cached = get_cached_popularity(cursor, person_id)
    if cached is not None:
        return cached
    popularity = get_popularity_score(url)
    if popularity is not None:
        name_tag = soup.select('a[data-testid="title-cast-item__actor"]')[actor_num-1]
        save_popularity(cursor, conn, person_id, name_tag.text.strip(), popularity)
    return popularity

def get_popularity_director(soup, cursor, conn):
    url, person_id = get_person_url(soup,'li[data-testid="title-pc-principal-credit"] a')
    if not url or not person_id:
        return None
    cached = get_cached_popularity(cursor, person_id)
    if cached is not None:
        return cached
    popularity = get_popularity_score(url)
    if popularity is not None:
        name_tag = soup.select('li[data-testid="title-pc-principal-credit"] a')[0]
        save_popularity(cursor, conn, person_id, name_tag.text.strip(), popularity)
    return popularity


# =========================
# MAIN FUNCTION
# =========================
def process_daily_batch(min_sleep=3, max_sleep=7):
    """
    Process a fixed number of movies each day.
    Automatically resumes from where it stopped.
    """

    # Connect to SQLite cache
    conn, cur = init_db()

    # Resume logic
    start_index = get_processed_count(cur)
    end_index = min(start_index + DAILY_BATCH_SIZE, len(df_links))

    # If all movies are processed, exit
    if start_index >= len(df_links):
        print("All movies processed.")
        conn.close()
        return len(df_links)

    print(f"Processing movies {start_index} to {end_index - 1}")

    # Loop through movies in the batch
    for i in range(start_index, end_index):
        print(f"Processing {i + 1}/{len(df_links)}: {df_links['movie_name'][i]}")

        # Fetch IMDb page
        soup = get_imdb_soup(df_links['link'][i])
        if not soup:
            print(f"Failed to fetch: {df_links['link'][i]}")
            continue

        # Extract tconst from link
        link = df_links['link'][i]
        tconst = link.split('/')[4]
        basics_row = df_basics.loc[tconst] if tconst in df_basics.index else {}
        ratings_row = df_ratings.loc[tconst] if tconst in df_ratings.index else {}

        # Prepare row for CSV
        row = {
            'movie_name': df_links['movie_name'][i],
            'budget': get_budget(soup),
            'gross': get_gross(soup),
            'genre': df_genres['genre'][i],
            'runtime': basics_row.get('runtimeMinutes', None),
            'rating': ratings_row.get('averageRating', None),
            'vote': ratings_row.get('numVotes', None),
            'pop_actor1': get_popularity_actor(soup, 1, cur, conn),
            'pop_actor2': get_popularity_actor(soup, 2, cur, conn),
            'pop_actor3': get_popularity_actor(soup, 3, cur, conn),
            'pop_director': get_popularity_director(soup, cur, conn),
            'link': df_links['link'][i]
        }

        save_movie_row(cur, conn, row)
        print("Saved", row['movie_name'])

        # Random sleep between movies to mimic human behavior
        sleep(uniform(min_sleep, max_sleep))

    print(f"Daily batch completed: movies {start_index}-{end_index - 1}")
    conn.close()
    return end_index

# =========================
# RUN
# =========================
if __name__ == "__main__":

    end_index = process_daily_batch(5, 10)
    if end_index == len(df_links):
        export_to_csv()