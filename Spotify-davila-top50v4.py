import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import pandas as pd
import psycopg2
from psycopg2 import extras
import os
from dotenv import load_dotenv
from datetime import date

load_dotenv('credenciales.env')

# credenciales Spotify
CLIENT_ID = os.getenv('SPOTIFY_CLIENT_ID')
CLIENT_SECRET = os.getenv('SPOTIFY_CLIENT_SECRET')

# credenciales Amazon Redshift
HOST = os.getenv('REDSHIFT_HOST')
PORT = os.getenv('REDSHIFT_PORT')
USER = os.getenv('REDSHIFT_USER')
PASSWORD = os.getenv('REDSHIFT_PASSWORD')
DATABASE = os.getenv('REDSHIFT_DATABASE')

# objeto Spotify
client_credentials_manager = SpotifyClientCredentials(client_id=CLIENT_ID, client_secret=CLIENT_SECRET)
sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

# consulta a la API de Spotify de lista de reproducción TOP 50 Argentina
results = sp.playlist_tracks('37i9dQZEVXbMMy2roB9myp')

# datos de canciones
tracks = results['items']

# dataframe con los datos de canciones
tracks_data = []
for track in tracks:
    track_data = {
        'id': track['track']['id'],
        'name': track['track']['name'],
        'artist': track['track']['artists'][0]['name'],
        'artist_id': track['track']['artists'][0]['id'],
        'album': track['track']['album']['name'],
        'popularity': track['track']['popularity'],
        'duration_ms': track['track']['duration_ms']
    }
    tracks_data.append(track_data)
tracks_df = pd.DataFrame(tracks_data)

# conexión con Amazon Redshift
conn = psycopg2.connect(
    host=HOST,
    port=PORT,
    user=USER,
    password=PASSWORD,
    dbname=DATABASE
)

# consulta SQL para crear la tabla de canciones
create_table_tracks_query = """
CREATE TABLE IF NOT EXISTS davila_spotify_tracks (
    id VARCHAR(255) PRIMARY KEY,
    name VARCHAR(255),
    artist VARCHAR(255),
    artist_id VARCHAR(255),
    album VARCHAR(255),
    popularity INT,
    duration_ms INT,
    date DATE
);
"""

# consulta SQL para crear la tabla de artistas
create_table_artists_query = """
CREATE TABLE IF NOT EXISTS davila_spotify_artists (
    id VARCHAR(255) PRIMARY KEY,
    name VARCHAR(255),
    followers INT,
    genres VARCHAR(255),
    popularity INT
);
"""



# crear la tabla de canciones
with conn.cursor() as cursor:
    cursor.execute(create_table_tracks_query)
    conn.commit()

 #crear la tabla de artistas
with conn.cursor() as cursor:
    cursor.execute(create_table_artists_query)
    conn.commit()

# consulta SQL para insertar los datos de canciones en la tabla tracks
insert_tracks_query = """
INSERT INTO davila_spotify_tracks (id, name, artist, artist_id, album, popularity, duration_ms, date)
VALUES %s;
"""

# consulta SQL para insertar los datos de artistas en la tabla artists
insert_artists_query = """
INSERT INTO davila_spotify_artists (id, name, followers, genres, popularity) VALUES %s;
"""

# consulta SQL para verificar si el artista ya existe en la tabla
check_artist_query = """
SELECT id FROM davila_spotify_artists WHERE id = %s;
"""

# artistas ya insertados
existing_artists = set()

# dataframes para los datos de canciones y artistas
data_values_tracks = []
data_values_artists = []

# consulta a la base de datos para obtener los artistas existentes
existing_artists_query = "SELECT id FROM davila_spotify_artists;"
existing_artists_df = pd.read_sql(existing_artists_query, conn)
    

for _, row in tracks_df.iterrows():
    track_data = (
        row['id'],
        row['name'],
        row['artist'],
        row['artist_id'],
        row['album'],
        row['popularity'],
        row['duration_ms'],
        date.today()
    )
    data_values_tracks.append(track_data)
    artist_id = row['artist_id']

    # verificar si el artista ya existe 
    if artist_id in existing_artists or artist_id in existing_artists_df['id'].values:
        continue

    # verificar si el artista ya existe en la tabla
    with conn.cursor() as cursor:
        cursor.execute(check_artist_query, (artist_id,))
        existing_artist = cursor.fetchone()

    if not existing_artist:
        # obtener información adicional del artista
        artist = sp.artist(artist_id)
        artist_data = (
            artist['id'],
            artist['name'],
            artist['followers']['total'],
            ",".join(artist['genres']),
            artist['popularity']
        )
        data_values_artists.append(artist_data)

        # agregar el artista al conjunto de artistas
        existing_artists.add(artist_id)

# insertar los datos de canciones en la tabla
with conn.cursor() as cursor:
    psycopg2.extras.execute_values(cursor, insert_tracks_query, data_values_tracks)
    conn.commit()

# insertar los datos de artistas en la tabla
if data_values_artists:
    with conn.cursor() as cursor:
        psycopg2.extras.execute_values(cursor, insert_artists_query, data_values_artists)
        conn.commit()
