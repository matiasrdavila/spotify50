import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import psycopg2
from psycopg2 import extras
import os
from dotenv import load_dotenv

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

# instancia objeto Spotify 
client_credentials_manager = SpotifyClientCredentials(client_id=CLIENT_ID, client_secret=CLIENT_SECRET)
sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

# consulta a la API de Spotify de lista de reproducción TOP 50 Argentina
results = sp.playlist_tracks('37i9dQZEVXbMMy2roB9myp')

# datos de canciones
tracks = results['items']

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
CREATE TABLE davila_spotify_tracks (
    id VARCHAR(255) PRIMARY KEY,
    name VARCHAR(255),
    artist VARCHAR(255),
    album VARCHAR(255),
    popularity INT,
    duration_ms INT
);
"""

# consulta SQL para crear la tabla de artistas
create_table_artists_query = """
CREATE TABLE davila_spotify_artists (
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

# crear la tabla de artistas
with conn.cursor() as cursor:
    cursor.execute(create_table_artists_query)
    conn.commit()

# consulta SQL para insertar los datos de canciones en la tabla tracks
insert_tracks_query = """
INSERT INTO davila_spotify_tracks (id, name, artist, album, popularity, duration_ms) VALUES %s;
"""

# consulta SQL para insertar los datos de artistas en la tabla artists
insert_artists_query = """
INSERT INTO davila_spotify_artists (id, name, followers, genres, popularity) VALUES %s;
"""

# for para obtener información y guardarla en las tablas
data_values_tracks = []
data_values_artists = []

for track in tracks:
    track_data = (
        track['track']['id'],
        track['track']['name'],
        track['track']['artists'][0]['name'],
        track['track']['album']['name'],
        track['track']['popularity'],
        track['track']['duration_ms']
    )
    data_values_tracks.append(track_data)

    # obtener información adicional del artista API
    artist = sp.artist(track['track']['artists'][0]['id'])
    artist_data = (
        artist['id'],
        artist['name'],
        artist['followers']['total'],
        ",".join(artist['genres']),
        artist['popularity']
    )
    data_values_artists.append(artist_data)

# insertar los datos de canciones en la tabla
with conn.cursor() as cursor:
    psycopg2.extras.execute_values(cursor, insert_tracks_query, data_values_tracks)
    conn.commit()

# insertar los datos de artistas en la tabla
with conn.cursor() as cursor:
    psycopg2.extras.execute_values(cursor, insert_artists_query, data_values_artists)
    conn.commit()
