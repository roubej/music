-- ============================================================
-- Application de gestion de playlists musicales - Apache Cassandra
-- Principe : une table par requête (modélisation orientée requêtes)
-- ============================================================

CREATE KEYSPACE IF NOT EXISTS music_app
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
-- RF=1 pour un noeud local. En production : RF=3 et NetworkTopologyStrategy

USE music_app;

-- ------------------------------------------------------------
-- 1. UTILISATEURS
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS users (
    user_id       uuid PRIMARY KEY,
    username      text,
    email         text,
    display_name  text,
    country       text,
    created_at    timestamp
);

-- Recherche par username (et garantie d'unicité via IF NOT EXISTS)
CREATE TABLE IF NOT EXISTS users_by_username (
    username text PRIMARY KEY,
    user_id  uuid
);

-- ------------------------------------------------------------
-- 2. CHANSONS
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS songs (
    song_id          uuid PRIMARY KEY,
    title            text,
    artist           text,
    album            text,
    genre            text,
    duration_seconds int,
    release_year     int,
    created_at       timestamp
);

-- Recherche par artiste
CREATE TABLE IF NOT EXISTS songs_by_artist (
    artist           text,
    title            text,
    song_id          uuid,
    album            text,
    genre            text,
    duration_seconds int,
    PRIMARY KEY ((artist), title, song_id)
);

-- Recherche par genre (les très gros genres pourraient être "bucketisés")
CREATE TABLE IF NOT EXISTS songs_by_genre (
    genre            text,
    title            text,
    song_id          uuid,
    artist           text,
    duration_seconds int,
    PRIMARY KEY ((genre), title, song_id)
);

-- Recherche par titre exact (titre normalisé en minuscules côté application)
CREATE TABLE IF NOT EXISTS songs_by_title (
    title_lower text,
    song_id     uuid,
    title       text,
    artist      text,
    genre       text,
    PRIMARY KEY ((title_lower), song_id)
);

-- ------------------------------------------------------------
-- 3. PLAYLISTS
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS playlists (
    playlist_id uuid PRIMARY KEY,
    user_id     uuid,
    name        text,
    description text,
    is_public   boolean,
    created_at  timestamp,
    updated_at  timestamp
);

-- Playlists d'un utilisateur (les plus récentes d'abord)
CREATE TABLE IF NOT EXISTS playlists_by_user (
    user_id     uuid,
    playlist_id timeuuid,
    name        text,
    description text,
    is_public   boolean,
    created_at  timestamp,
    PRIMARY KEY ((user_id), playlist_id)
) WITH CLUSTERING ORDER BY (playlist_id DESC);

-- Playlists publiques visibles par tous
CREATE TABLE IF NOT EXISTS public_playlists (
    playlist_id uuid PRIMARY KEY,
    user_id     uuid,
    name        text,
    description text,
    created_at  timestamp
);

-- ------------------------------------------------------------
-- 4. CHANSONS D'UNE PLAYLIST (ordre géré par "position")
-- Positions espacées (1000, 2000, 3000...) : insérer entre deux
-- morceaux = position médiane, sans tout renuméroter.
-- title/artist dupliqués pour éviter toute jointure à l'affichage.
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS playlist_songs (
    playlist_id      uuid,
    position         int,
    song_id          uuid,
    title            text,
    artist           text,
    duration_seconds int,
    added_at         timestamp,
    PRIMARY KEY ((playlist_id), position)
);

-- Index inverse : retrouver la position d'une chanson pour la supprimer/déplacer
CREATE TABLE IF NOT EXISTS playlist_song_positions (
    playlist_id uuid,
    song_id     uuid,
    position    int,
    PRIMARY KEY ((playlist_id), song_id)
);

-- ------------------------------------------------------------
-- 5. FAVORIS
-- ------------------------------------------------------------
-- Affichage : plus récents d'abord
CREATE TABLE IF NOT EXISTS favorites_by_user (
    user_id  uuid,
    added_at timestamp,
    song_id  uuid,
    title    text,
    artist   text,
    PRIMARY KEY ((user_id), added_at, song_id)
) WITH CLUSTERING ORDER BY (added_at DESC, song_id ASC);

-- Test d'appartenance + suppression (retrouver added_at)
CREATE TABLE IF NOT EXISTS favorite_lookup (
    user_id  uuid,
    song_id  uuid,
    added_at timestamp,
    PRIMARY KEY ((user_id), song_id)
);

-- Historique d'écoute d'un utilisateur
CREATE TABLE IF NOT EXISTS listens_by_user (
    user_id     uuid,
    listened_at timestamp,
    song_id     uuid,
    title       text,
    artist      text,
    genre       text,
    PRIMARY KEY ((user_id), listened_at)
) WITH CLUSTERING ORDER BY (listened_at DESC);

-- ============================================================
-- REQUÊTES PAR FONCTIONNALITÉ
-- ============================================================

-- Créer un utilisateur (unicité du username)
-- INSERT INTO users_by_username (username, user_id) VALUES (?, ?) IF NOT EXISTS;
-- INSERT INTO users (user_id, username, email, display_name, country, created_at)
--   VALUES (?, ?, ?, ?, ?, toTimestamp(now()));
-- Consulter un profil :
-- SELECT * FROM users WHERE user_id = ?;

-- Ajouter une chanson (BATCH logué : 4 tables synchronisées)
-- BEGIN BATCH
--   INSERT INTO songs (...) VALUES (...);
--   INSERT INTO songs_by_artist (...) VALUES (...);
--   INSERT INTO songs_by_genre (...) VALUES (...);
--   INSERT INTO songs_by_title (...) VALUES (...);
-- APPLY BATCH;

-- Recherche musicale
-- SELECT * FROM songs_by_title  WHERE title_lower = 'bohemian rhapsody';
-- SELECT * FROM songs_by_artist WHERE artist = 'Queen';
-- SELECT * FROM songs_by_genre  WHERE genre = 'rock' LIMIT 50;

-- Créer une playlist
-- BEGIN BATCH
--   INSERT INTO playlists (...) VALUES (...);
--   INSERT INTO playlists_by_user (...) VALUES (...);
-- APPLY BATCH;

-- Lister les playlists d'un utilisateur
-- SELECT * FROM playlists_by_user WHERE user_id = ?;

-- Afficher les chansons d'une playlist (déjà triées)
-- SELECT * FROM playlist_songs WHERE playlist_id = ?;

-- Ajouter une chanson en fin de playlist (position = dernière + 1000)
-- SELECT position FROM playlist_songs WHERE playlist_id = ? ORDER BY position DESC LIMIT 1;
-- BEGIN BATCH
--   INSERT INTO playlist_songs (playlist_id, position, song_id, title, artist, duration_seconds, added_at) VALUES (...);
--   INSERT INTO playlist_song_positions (playlist_id, song_id, position) VALUES (?, ?, ?);
-- APPLY BATCH;

-- Supprimer une chanson
-- SELECT position FROM playlist_song_positions WHERE playlist_id = ? AND song_id = ?;
-- BEGIN BATCH
--   DELETE FROM playlist_songs WHERE playlist_id = ? AND position = ?;
--   DELETE FROM playlist_song_positions WHERE playlist_id = ? AND song_id = ?;
-- APPLY BATCH;

-- Déplacer une chanson (nouvelle position = médiane entre ses futurs voisins)
-- BEGIN BATCH
--   DELETE FROM playlist_songs WHERE playlist_id = ? AND position = :ancienne;
--   INSERT INTO playlist_songs (...) VALUES (?, :nouvelle, ...);
--   UPDATE playlist_song_positions SET position = :nouvelle WHERE playlist_id = ? AND song_id = ?;
-- APPLY BATCH;

-- Favoris
-- Ajouter :
-- BEGIN BATCH
--   INSERT INTO favorites_by_user (user_id, added_at, song_id, title, artist) VALUES (?, toTimestamp(now()), ?, ?, ?);
--   INSERT INTO favorite_lookup (user_id, song_id, added_at) VALUES (?, ?, ?);
-- APPLY BATCH;
-- Retirer : lire added_at dans favorite_lookup, puis
-- BEGIN BATCH
--   DELETE FROM favorites_by_user WHERE user_id = ? AND added_at = ? AND song_id = ?;
--   DELETE FROM favorite_lookup  WHERE user_id = ? AND song_id = ?;
-- APPLY BATCH;
-- Lister :
-- SELECT * FROM favorites_by_user WHERE user_id = ?;
