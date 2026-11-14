"""Données de test pour l'application de playlists (Cassandra).

Usage :  python seed.py          (ajoute les données si le catalogue est vide)
         python seed.py --force  (ajoute quand même)
Variables : CASSANDRA_HOST (défaut 127.0.0.1), CASSANDRA_PORT (défaut 9042)
"""
import os
import re
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

from cassandra.cluster import Cluster
from cassandra.query import BatchStatement

KEYSPACE = "music_app"
GAP = 1000
SCHEMA_FILE = Path(__file__).with_name("schema.cql")

# (titre, artiste, album, genre, durée en secondes, année)
SONGS = [
    ("Bohemian Rhapsody", "Queen", "A Night at the Opera", "rock", 355, 1975),
    ("Don't Stop Me Now", "Queen", "Jazz", "rock", 209, 1979),
    ("Hotel California", "Eagles", "Hotel California", "rock", 391, 1976),
    ("Stairway to Heaven", "Led Zeppelin", "Led Zeppelin IV", "rock", 482, 1971),
    ("Smells Like Teen Spirit", "Nirvana", "Nevermind", "rock", 301, 1991),
    ("Sweet Child O' Mine", "Guns N' Roses", "Appetite for Destruction", "rock", 356, 1987),
    ("Back in Black", "AC/DC", "Back in Black", "rock", 255, 1980),
    ("Billie Jean", "Michael Jackson", "Thriller", "pop", 294, 1982),
    ("Thriller", "Michael Jackson", "Thriller", "pop", 357, 1982),
    ("Like a Prayer", "Madonna", "Like a Prayer", "pop", 341, 1989),
    ("Shape of You", "Ed Sheeran", "÷", "pop", 234, 2017),
    ("Blinding Lights", "The Weeknd", "After Hours", "pop", 200, 2019),
    ("Levitating", "Dua Lipa", "Future Nostalgia", "pop", 203, 2020),
    ("Rolling in the Deep", "Adele", "21", "pop", 228, 2010),
    ("Someone Like You", "Adele", "21", "pop", 285, 2011),
    ("Uptown Funk", "Mark Ronson", "Uptown Special", "funk", 270, 2014),
    ("Superstition", "Stevie Wonder", "Talking Book", "funk", 245, 1972),
    ("Get Up (I Feel Like Being a) Sex Machine", "James Brown", "Sex Machine", "funk", 330, 1970),
    ("Lose Yourself", "Eminem", "8 Mile", "hip-hop", 326, 2002),
    ("Sicko Mode", "Travis Scott", "Astroworld", "hip-hop", 312, 2018),
    ("HUMBLE.", "Kendrick Lamar", "DAMN.", "hip-hop", 177, 2017),
    ("Juicy", "The Notorious B.I.G.", "Ready to Die", "hip-hop", 302, 1994),
    ("Alright", "Kendrick Lamar", "To Pimp a Butterfly", "hip-hop", 219, 2015),
    ("So What", "Miles Davis", "Kind of Blue", "jazz", 562, 1959),
    ("Take Five", "Dave Brubeck", "Time Out", "jazz", 324, 1959),
    ("My Favorite Things", "John Coltrane", "My Favorite Things", "jazz", 828, 1961),
    ("Fly Me to the Moon", "Frank Sinatra", "It Might as Well Be Swing", "jazz", 148, 1964),
    ("What a Wonderful World", "Louis Armstrong", "What a Wonderful World", "jazz", 139, 1967),
    ("Clair de Lune", "Claude Debussy", "Suite bergamasque", "classique", 300, 1905),
    ("Für Elise", "Ludwig van Beethoven", "Bagatelles", "classique", 180, 1810),
    ("Four Seasons: Spring", "Antonio Vivaldi", "Le quattro stagioni", "classique", 600, 1725),
    ("Gymnopédie No.1", "Erik Satie", "Gymnopédies", "classique", 190, 1888),
    ("Symphony No. 5", "Ludwig van Beethoven", "Symphony No. 5", "classique", 450, 1808),
    ("One More Time", "Daft Punk", "Discovery", "electro", 320, 2000),
    ("Around the World", "Daft Punk", "Homework", "electro", 429, 1997),
    ("Levels", "Avicii", "Levels", "electro", 202, 2011),
    ("Wake Me Up", "Avicii", "True", "electro", 247, 2013),
    ("Titanium", "David Guetta", "Nothing but the Beat", "electro", 245, 2011),
    ("No Woman, No Cry", "Bob Marley", "Natty Dread", "reggae", 235, 1974),
    ("Is This Love", "Bob Marley", "Kaya", "reggae", 232, 1978),
    ("Could You Be Loved", "Bob Marley", "Uprising", "reggae", 237, 1980),
    ("Jolene", "Dolly Parton", "Jolene", "country", 162, 1973),
    ("Ring of Fire", "Johnny Cash", "Ring of Fire", "country", 157, 1963),
    ("Take Me Home, Country Roads", "John Denver", "Poems, Prayers and Promises", "country", 195, 1971),
    ("Ne me quitte pas", "Jacques Brel", "La Valse à mille temps", "chanson", 235, 1959),
    ("La Vie en rose", "Édith Piaf", "La Vie en rose", "chanson", 187, 1947),
    ("Non, je ne regrette rien", "Édith Piaf", "Non, je ne regrette rien", "chanson", 140, 1960),
    ("Alors on danse", "Stromae", "Cheese", "pop", 207, 2009),
    ("Papaoutai", "Stromae", "Racine carrée", "pop", 232, 2013),
    ("Dernière danse", "Indila", "Mini World", "pop", 213, 2014),
    ("Despacito", "Luis Fonsi", "Vida", "latin", 229, 2017),
    ("Mi Gente", "J Balvin", "Vibras", "latin", 189, 2017),
    ("Shakira: Hips Don't Lie", "Shakira", "Oral Fixation Vol. 2", "latin", 218, 2006),
    ("Redemption Song", "Bob Marley", "Uprising", "reggae", 227, 1980),
    ("Imagine", "John Lennon", "Imagine", "rock", 187, 1971),
    ("Let It Be", "The Beatles", "Let It Be", "rock", 243, 1970),
    ("Yesterday", "The Beatles", "Help!", "rock", 125, 1965),
    ("Come Together", "The Beatles", "Abbey Road", "rock", 259, 1969),
    ("Wonderwall", "Oasis", "(What's the Story) Morning Glory?", "rock", 258, 1995),
    ("Creep", "Radiohead", "Pablo Honey", "rock", 238, 1992),
    ("Seven Nation Army", "The White Stripes", "Elephant", "rock", 232, 2003),
    ("Mr. Brightside", "The Killers", "Hot Fuss", "rock", 223, 2003),
    ("Believer", "Imagine Dragons", "Evolve", "rock", 204, 2017),
    ("Perfect", "Ed Sheeran", "÷", "pop", 263, 2017),
]

USERS = [
    ("roua", "Roua", "roua@example.com", "Tunisie"),
    ("sami", "Sami", "sami@example.com", "France"),
    ("lina", "Lina", "lina@example.com", "Canada"),
]

# playlist : (utilisateur, nom, description, [titres])
PLAYLISTS = [
    ("roua", "Road trip", "Pour la route", ["Hotel California", "Back in Black", "Take Me Home, Country Roads", "Wonderwall", "Seven Nation Army", "Mr. Brightside"]),
    ("roua", "Étude & calme", "Concentration", ["Clair de Lune", "Gymnopédie No.1", "Take Five", "So What", "Für Elise"]),
    ("sami", "Soirée", "Ambiance fête", ["Uptown Funk", "Levels", "One More Time", "Alors on danse", "Titanium", "Blinding Lights"]),
    ("lina", "Hip-hop mix", "", ["Lose Yourself", "HUMBLE.", "Juicy", "Alright", "Sicko Mode"]),
]

FAVORITES = {
    "roua": ["Bohemian Rhapsody", "Clair de Lune", "No Woman, No Cry", "Papaoutai"],
    "sami": ["Billie Jean", "One More Time", "Despacito"],
    "lina": ["Lose Yourself", "La Vie en rose", "Rolling in the Deep"],
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


def main():
    force = "--force" in sys.argv
    session = Cluster([os.getenv("CASSANDRA_HOST", "127.0.0.1")],
                      port=int(os.getenv("CASSANDRA_PORT", "9042"))).connect()
    init_schema(session)
    session.set_keyspace(KEYSPACE)

    if not force and session.execute("SELECT song_id FROM songs LIMIT 1").one():
        print("Le catalogue contient déjà des chansons. Utilisez --force pour ajouter quand même.")
        return

    P = session.prepare
    song_ins = P("INSERT INTO songs (song_id, title, artist, album, genre, duration_seconds, release_year, created_at) VALUES (?,?,?,?,?,?,?,?)")
    song_artist = P("INSERT INTO songs_by_artist (artist, title, song_id, album, genre, duration_seconds) VALUES (?,?,?,?,?,?)")
    song_genre = P("INSERT INTO songs_by_genre (genre, title, song_id, artist, duration_seconds) VALUES (?,?,?,?,?)")
    song_title = P("INSERT INTO songs_by_title (title_lower, song_id, title, artist, genre) VALUES (?,?,?,?,?)")
    user_claim = P("INSERT INTO users_by_username (username, user_id) VALUES (?, ?) IF NOT EXISTS")
    user_ins = P("INSERT INTO users (user_id, username, email, display_name, country, created_at) VALUES (?,?,?,?,?,?)")
    user_get = P("SELECT user_id FROM users_by_username WHERE username = ?")
    pl_ins = P("INSERT INTO playlists (playlist_id, user_id, name, description, created_at, updated_at) VALUES (?,?,?,?,?,?)")
    pl_user = P("INSERT INTO playlists_by_user (user_id, playlist_id, name, description, created_at) VALUES (?,?,?,?,?)")
    pls_ins = P("INSERT INTO playlist_songs (playlist_id, position, song_id, title, artist, duration_seconds, added_at) VALUES (?,?,?,?,?,?,?)")
    plp_ins = P("INSERT INTO playlist_song_positions (playlist_id, song_id, position) VALUES (?,?,?)")
    fav_ins = P("INSERT INTO favorites_by_user (user_id, added_at, song_id, title, artist) VALUES (?,?,?,?,?)")
    fav_look = P("INSERT INTO favorite_lookup (user_id, song_id, added_at) VALUES (?,?,?)")

    now = datetime.now(timezone.utc)

    # chansons
    by_title = {}
    for title, artist, album, genre, dur, year in SONGS:
        sid = uuid.uuid4()
        b = BatchStatement()
        b.add(song_ins, (sid, title, artist, album, genre, dur, year, now))
        b.add(song_artist, (artist, title, sid, album, genre, dur))
        b.add(song_genre, (genre, title, sid, artist, dur))
        b.add(song_title, (title.lower(), sid, title, artist, genre))
        session.execute(b)
        by_title[title] = (sid, artist, dur)
    print(f"{len(SONGS)} chansons ajoutées")

    # utilisateurs
    uids = {}
    for username, display, email, country in USERS:
        uid = uuid.uuid4()
        if session.execute(user_claim, (username, uid)).one().applied:
            session.execute(user_ins, (uid, username, email, display, country, now))
            uids[username] = uid
        else:
            uids[username] = session.execute(user_get, (username,)).one().user_id
    print(f"{len(USERS)} utilisateurs prêts")

    # playlists
    for username, name, desc, titles in PLAYLISTS:
        uid, pid = uids[username], uuid.uuid1()
        b = BatchStatement()
        b.add(pl_ins, (pid, uid, name, desc, now, now))
        b.add(pl_user, (uid, pid, name, desc, now))
        session.execute(b)
        for i, t in enumerate(titles, start=1):
            sid, artist, dur = by_title[t]
            b = BatchStatement()
            b.add(pls_ins, (pid, i * GAP, sid, t, artist, dur, now))
            b.add(plp_ins, (pid, sid, i * GAP))
            session.execute(b)
    print(f"{len(PLAYLISTS)} playlists ajoutées")

    # favoris
    for username, titles in FAVORITES.items():
        uid = uids[username]
        for t in titles:
            if t not in by_title:
                continue
            sid, artist, _ = by_title[t]
            ts = datetime.now(timezone.utc)
            b = BatchStatement()
            b.add(fav_ins, (uid, ts, sid, t, artist))
            b.add(fav_look, (uid, sid, ts))
            session.execute(b)
    print("Favoris ajoutés. Terminé.")


if __name__ == "__main__":
    main()
