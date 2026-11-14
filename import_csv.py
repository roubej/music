"""Import d'un CSV de chansons (ex. Kaggle "Spotify Tracks Dataset") dans Cassandra.

Usage :
    python import_csv.py chemin/vers/dataset.csv [--limit 50000]

Colonnes reconnues automatiquement (insensible à la casse) :
    titre  : track_name | name | title | song
    artiste: artists | artist | artist_name   (si plusieurs, séparés par ';' ou ',' -> le premier est gardé)
    album  : album_name | album
    genre  : track_genre | genre | genres
    durée  : duration_ms (millisecondes) | duration_seconds | duration
    année  : year | release_year
    id     : track_id | id  (rend l'import rejouable sans doublons)

Variables : CASSANDRA_HOST (défaut 127.0.0.1), CASSANDRA_PORT (défaut 9042)
"""
import csv
import os
import re
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

from cassandra.cluster import Cluster
from cassandra.concurrent import execute_concurrent_with_args

KEYSPACE = "music_app"
SCHEMA_FILE = Path(__file__).with_name("schema.cql")
NAMESPACE = uuid.UUID("6f1c2d3e-4a5b-4c6d-8e7f-0a1b2c3d4e5f")

ALIASES = {
    "title": ["track_name", "name", "title", "song", "song_name"],
    "artist": ["artists", "artist", "artist_name", "artist_names"],
    "album": ["album_name", "album", "album_title"],
    "genre": ["track_genre", "genre", "genres", "playlist_genre"],
    "duration_ms": ["duration_ms", "duration_ms_x"],
    "duration_s": ["duration_seconds", "duration_sec", "duration"],
    "year": ["year", "release_year"],
    "track_id": ["track_id", "id", "spotify_id"],
}


def init_schema(session):
    lines = [l for l in SCHEMA_FILE.read_text(encoding="utf-8").splitlines()
             if not l.lstrip().startswith("--")]
    for stmt in "\n".join(lines).split(";"):
        stmt = stmt.strip()
        if not stmt:
            continue
        m = re.match(r"USE\s+(\w+)", stmt, re.I)
        if m:
            session.set_keyspace(m.group(1))
        else:
            session.execute(stmt)


def find_columns(header):
    lower = {h.strip().lower(): h for h in header}
    return {key: next((lower[a] for a in names if a in lower), None) for key, names in ALIASES.items()}


def to_int(v):
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return None


def first_artist(v):
    return re.split(r"[;,]", v)[0].strip()


def main():
    args = sys.argv[1:]
    if not args:
        print(__doc__)
        sys.exit(1)
    path = args[0]
    limit = int(args[args.index("--limit") + 1]) if "--limit" in args else None

    with open(path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        cols = find_columns(reader.fieldnames or [])
        missing = [k for k in ("title", "artist", "genre") if not cols[k]]
        if missing:
            print(f"Colonnes introuvables pour : {', '.join(missing)}")
            print(f"Colonnes du fichier : {reader.fieldnames}")
            sys.exit(1)
        print("Colonnes utilisées :", {k: v for k, v in cols.items() if v})

        songs, seen = [], set()
        for row in reader:
            title = (row.get(cols["title"]) or "").strip()
            artist = first_artist(row.get(cols["artist"]) or "")
            genre = (row.get(cols["genre"]) or "").strip().lower()
            if not (title and artist and genre):
                continue
            key = (title.lower(), artist.lower())
            if key in seen:
                continue
            seen.add(key)
            album = (row.get(cols["album"]) or "").strip() if cols["album"] else ""
            dur = None
            if cols["duration_ms"]:
                ms = to_int(row.get(cols["duration_ms"]))
                dur = ms // 1000 if ms else None
            elif cols["duration_s"]:
                dur = to_int(row.get(cols["duration_s"]))
            year = to_int(row.get(cols["year"])) if cols["year"] else None
            raw_id = (row.get(cols["track_id"]) or "") if cols["track_id"] else ""
            sid = uuid.uuid5(NAMESPACE, raw_id or f"{title.lower()}|{artist.lower()}")
            songs.append((sid, title, artist, album, genre, dur, year))
            if limit and len(songs) >= limit:
                break

    print(f"{len(songs)} chansons uniques à importer")
    if not songs:
        return

    session = Cluster([os.getenv("CASSANDRA_HOST", "127.0.0.1")],
                      port=int(os.getenv("CASSANDRA_PORT", "9042"))).connect()
    init_schema(session)
    session.set_keyspace(KEYSPACE)
    P = session.prepare
    now = datetime.now(timezone.utc)

    jobs = [
        (P("INSERT INTO songs (song_id, title, artist, album, genre, duration_seconds, release_year, created_at) VALUES (?,?,?,?,?,?,?,?)"),
         [(s, t, a, al, g, d, y, now) for s, t, a, al, g, d, y in songs]),
        (P("INSERT INTO songs_by_artist (artist, title, song_id, album, genre, duration_seconds) VALUES (?,?,?,?,?,?)"),
         [(a, t, s, al, g, d) for s, t, a, al, g, d, y in songs]),
        (P("INSERT INTO songs_by_genre (genre, title, song_id, artist, duration_seconds) VALUES (?,?,?,?,?)"),
         [(g, t, s, a, d) for s, t, a, al, g, d, y in songs]),
        (P("INSERT INTO songs_by_title (title_lower, song_id, title, artist, genre) VALUES (?,?,?,?,?)"),
         [(t.lower(), s, t, a, g) for s, t, a, al, g, d, y in songs]),
    ]
    names = ["songs", "songs_by_artist", "songs_by_genre", "songs_by_title"]
    for name, (stmt, params) in zip(names, jobs):
        results = execute_concurrent_with_args(session, stmt, params, concurrency=100, raise_on_first_error=False)
        errors = [r for ok, r in results if not ok]
        print(f"{name}: {len(params) - len(errors)} OK, {len(errors)} erreurs")
        if errors:
            print("  première erreur :", errors[0])
    print("Import terminé.")


if __name__ == "__main__":
    main()
