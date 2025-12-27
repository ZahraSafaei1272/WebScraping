import pandas as pd
import re
import os


# -------------------------------
DATA_PATH = "title.basics.tsv"
df = pd.read_csv('imdb_titles_links.csv')
links = df['link']
titles = df['movie_name']
filename = 'genres.csv'

def save_to_csv(title, url, genres, filename):
    """Append data from movie links to the CSV file."""
    data = pd.DataFrame(
                    {'movie_name': [title],
                     'link': [url],
                     'genre': [genres],
                }
    )
    # Append data without overwriting previous entries
    data.to_csv(filename, mode='a', index=False, header=not os.path.exists(filename))

# -------------------------------
# extract the movie ID from any link
def extract_imdb_id(url):
    match = re.search(r"tt\d+", url)
    return match.group(0) if match else None

df_imdb = pd.read_csv(DATA_PATH, sep="\t", dtype=str, na_values="\\N")
df_imdb = df_imdb[df_imdb["titleType"] == "movie"]  # just movies

for i, url in enumerate(links):
    title = titles[i]
    imdb_id = extract_imdb_id(url)
    filtered = df_imdb[df_imdb["tconst"] == imdb_id]["genres"]
    if not filtered.empty:
        genres = filtered.iloc[0]
    else:
        genres = 'None'
    save_to_csv(title, url, genres, filename)
    print(f'done{i}')
