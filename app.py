"""Gestionnaire de playlists musicales - Streamlit + Apache Cassandra."""
import os
import re
import uuid
from datetime import datetime, timezone
from pathlib import Path

import streamlit as st
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement

KEYSPACE = "music_app"
GAP = 1000  # espacement des positions dans une playlist
SCHEMA_FILE = Path(__file__).with_name("schema.cql")

st.set_page_config(page_title="Playlists Cassandra", page_icon="🎵", layout="wide")

st.markdown(
    """
    <style>
        :root {
            --bg: #0b1020;
            --panel: #121b2d;
            --panel-strong: #1a2640;
            --panel-muted: #0f172a;
            --text: #e8eefc;
            --muted: #a8b5d1;
            --primary: #7c9cff;
            --primary-strong: #6079ff;
            --success: #4ade80;
            --warning: #fbbf24;
            --danger: #f87171;
            --shadow: rgba(15, 23, 42, 0.45);
        }

        .stApp {
            background: linear-gradient(180deg, #09111d 0%, #10192d 100%);
            color: var(--text);
        }

        .stAppHeader {
            background: transparent;
        }

        [data-testid="stSidebar"] {
            background: linear-gradient(180deg, #0a1220 0%, #111d30 100%);
            border-right: 1px solid rgba(142, 167, 255, 0.18);
        }

        [data-testid="stSidebar"] section {
            padding-top: 1rem;
        }

        .block-container {
            padding-top: 2rem;
            padding-bottom: 2rem;
        }

        .music-header {
            background: linear-gradient(135deg, rgba(124, 156, 255, 0.18), rgba(74, 222, 128, 0.08));
            border: 1px solid rgba(124, 156, 255, 0.2);
            border-radius: 18px;
            padding: 1.25rem 1.5rem;
            box-shadow: 0 12px 30px var(--shadow);
            margin-bottom: 1.25rem;
        }

        .music-header h1 {
            margin: 0;
            font-size: clamp(2rem, 3vw, 2.6rem);
            letter-spacing: 0.02em;
        }

        .music-header p {
            margin: 0.5rem 0 0;
            color: var(--muted);
            font-size: 1rem;
        }

        div[data-testid="stTabs"] [role="tablist"] {
            gap: 0.6rem;
            border-bottom: 1px solid rgba(168, 181, 209, 0.18);
            padding-bottom: 0.25rem;
        }

        div[data-testid="stTabs"] [role="tab"] {
            background: rgba(17, 24, 39, 0.65);
            border: 1px solid rgba(168, 181, 209, 0.12);
            border-radius: 10px 10px 0 0;
            color: var(--muted);
            padding: 0.55rem 1rem;
            font-weight: 600;
        }

        div[data-testid="stTabs"] [role="tab"][aria-selected="true"] {
            background: linear-gradient(135deg, rgba(124, 156, 255, 0.18), rgba(255,255,255,0.04));
            color: var(--text);
            border-color: rgba(124, 156, 255, 0.35);
            box-shadow: inset 0 1px 0 rgba(255,255,255,0.08);
        }

        .stButton > button {
            border-radius: 10px;
            border: 1px solid rgba(124, 156, 255, 0.25);
            background: linear-gradient(180deg, rgba(30, 41, 59, 0.9), rgba(17, 24, 39, 0.95));
            color: var(--text);
            font-weight: 600;
            transition: 0.2s ease;
        }

        .stButton > button:hover {
            border-color: rgba(124, 156, 255, 0.55);
            box-shadow: 0 8px 20px rgba(96, 121, 255, 0.2);
            transform: translateY(-1px);
        }

        .stButton > button[kind="primary"] {
            background: linear-gradient(135deg, var(--primary), var(--primary-strong));
            color: white;
            border: none;
        }

        .stButton > button[kind="secondary"] {
            background: rgba(248, 113, 113, 0.08);
            border-color: rgba(248, 113, 113, 0.35);
            color: #fecaca;
        }

        .stTextInput > div > div > input,
        .stTextArea > div > textarea,
        .stNumberInput > div > div > input,
        .stSelectbox > div > div,
        .stMultiSelect > div > div {
            background: rgba(15, 23, 42, 0.85);
            color: var(--text);
            border: 1px solid rgba(168, 181, 209, 0.18);
            border-radius: 10px;
        }

        .stCheckbox {
            color: var(--text);
        }

        .stDataFrame, .stJson {
            border-radius: 12px;
            overflow: hidden;
        }

        .stAlert {
            border-radius: 12px;
        }

        [data-testid="stMetricValue"] {
            font-size: 1.6rem;
            font-weight: 700;
            color: var(--text);
        }

        .stMarkdown {
            color: var(--text);
        }
    </style>
    """,
    unsafe_allow_html=True,
)


# ---------------------------------------------------------------- connexion
@st.cache_resource(show_spinner="Connexion à Cassandra...")
def get_session():
    host = os.getenv("CASSANDRA_HOST", "127.0.0.1")
    port = int(os.getenv("CASSANDRA_PORT", "9042"))
    session = Cluster([host], port=port).connect()
    init_schema(session)
    ensure_playlist_columns(session)
    session.set_keyspace(KEYSPACE)
    return session


def ensure_playlist_columns(session):
    """Migrates legacy playlists tables to add is_public when upgrading an existing database."""
    session.set_keyspace(KEYSPACE)
    for table in ("playlists", "playlists_by_user"):
        columns = {
            row.column_name
            for row in session.execute(
                "SELECT column_name FROM system_schema.columns WHERE keyspace_name = %s AND table_name = %s ALLOW FILTERING",
                (KEYSPACE, table),
            )
        }
        if "is_public" not in columns:
            session.execute(f"ALTER TABLE {table} ADD is_public boolean")


def init_schema(session):
    """Exécute schema.cql (commentaires retirés) ; idempotent grâce à IF NOT EXISTS."""
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


@st.cache_resource
def prepared():
    s = get_session()
    p = s.prepare
    return {
        # utilisateurs
        "user_claim": p("INSERT INTO users_by_username (username, user_id) VALUES (?, ?) IF NOT EXISTS"),
        "user_ins": p("INSERT INTO users (user_id, username, email, display_name, country, created_at) VALUES (?,?,?,?,?,?)"),
        "user_get": p("SELECT * FROM users WHERE user_id = ?"),
        # chansons
        "song_ins": p("INSERT INTO songs (song_id, title, artist, album, genre, duration_seconds, release_year, created_at) VALUES (?,?,?,?,?,?,?,?)"),
        "song_artist": p("INSERT INTO songs_by_artist (artist, title, song_id, album, genre, duration_seconds) VALUES (?,?,?,?,?,?)"),
        "song_genre": p("INSERT INTO songs_by_genre (genre, title, song_id, artist, duration_seconds) VALUES (?,?,?,?,?)"),
        "song_title": p("INSERT INTO songs_by_title (title_lower, song_id, title, artist, genre) VALUES (?,?,?,?,?)"),
        "song_get": p("SELECT * FROM songs WHERE song_id = ?"),
        "q_title": p("SELECT song_id, title, artist, genre FROM songs_by_title WHERE title_lower = ?"),
        "q_artist": p("SELECT song_id, title, artist, genre FROM songs_by_artist WHERE artist = ?"),
        "q_genre": p("SELECT song_id, title, artist, genre FROM songs_by_genre WHERE genre = ? LIMIT 200"),
        # playlists
        "pl_ins": p("INSERT INTO playlists (playlist_id, user_id, name, description, is_public, created_at, updated_at) VALUES (?,?,?,?,?,?,?)"),
        "pl_user_ins": p("INSERT INTO playlists_by_user (user_id, playlist_id, name, description, is_public, created_at) VALUES (?,?,?,?,?,?)"),
        "pl_pub_upd": p("UPDATE playlists SET is_public = ? WHERE playlist_id = ?"),
        "pl_user_pub_upd": p("UPDATE playlists_by_user SET is_public = ? WHERE user_id = ? AND playlist_id = ?"),
        "pl_list": p("SELECT * FROM playlists_by_user WHERE user_id = ?"),
        "pl_del": p("DELETE FROM playlists WHERE playlist_id = ?"),
        "pl_user_del": p("DELETE FROM playlists_by_user WHERE user_id = ? AND playlist_id = ?"),
        "pub_ins": p("INSERT INTO public_playlists (playlist_id, user_id, name, description, created_at) VALUES (?,?,?,?,?) IF NOT EXISTS"),
        "pub_del": p("DELETE FROM public_playlists WHERE playlist_id = ?"),
        "pub_list": p("SELECT * FROM public_playlists LIMIT 100"),
        "pls_del_all": p("DELETE FROM playlist_songs WHERE playlist_id = ?"),
        "plp_del_all": p("DELETE FROM playlist_song_positions WHERE playlist_id = ?"),
        # contenu des playlists
        "pls_list": p("SELECT * FROM playlist_songs WHERE playlist_id = ?"),
        "pls_last": p("SELECT position FROM playlist_songs WHERE playlist_id = ? ORDER BY position DESC LIMIT 1"),
        "pls_ins": p("INSERT INTO playlist_songs (playlist_id, position, song_id, title, artist, duration_seconds, added_at) VALUES (?,?,?,?,?,?,?)"),
        "pls_del": p("DELETE FROM playlist_songs WHERE playlist_id = ? AND position = ?"),
        "plp_ins": p("INSERT INTO playlist_song_positions (playlist_id, song_id, position) VALUES (?,?,?)"),
        "plp_get": p("SELECT position FROM playlist_song_positions WHERE playlist_id = ? AND song_id = ?"),
        "plp_del": p("DELETE FROM playlist_song_positions WHERE playlist_id = ? AND song_id = ?"),
        # favoris
        "fav_ins": p("INSERT INTO favorites_by_user (user_id, added_at, song_id, title, artist) VALUES (?,?,?,?,?)"),
        "fav_look_ins": p("INSERT INTO favorite_lookup (user_id, song_id, added_at) VALUES (?,?,?)"),
        "fav_look_get": p("SELECT added_at FROM favorite_lookup WHERE user_id = ? AND song_id = ?"),
        "fav_list": p("SELECT * FROM favorites_by_user WHERE user_id = ?"),
        "fav_del": p("DELETE FROM favorites_by_user WHERE user_id = ? AND added_at = ? AND song_id = ?"),
        "fav_look_del": p("DELETE FROM favorite_lookup WHERE user_id = ? AND song_id = ?"),
        # historique d'écoute
        "listen_ins": p("INSERT INTO listens_by_user (user_id, listened_at, song_id, title, artist, genre) VALUES (?,?,?,?,?,?)"),
        "listen_list": p("SELECT * FROM listens_by_user WHERE user_id = ? LIMIT 20"),
    }


def now():
    return datetime.now(timezone.utc)


def run(name, *args):
    return get_session().execute(prepared()[name], args)


def batch(*ops):
    b = BatchStatement()
    for name, args in ops:
        b.add(prepared()[name], args)
    get_session().execute(b)


# ---------------------------------------------------------------- utilisateurs
def create_user(username, email, display_name, country):
    username = username.strip().lower()
    uid = uuid.uuid4()
    applied = run("user_claim", username, uid).one().applied
    if not applied:
        return None
    run("user_ins", uid, username, email, display_name or username, country, now())
    return uid


def list_users():
    return list(get_session().execute("SELECT user_id, username, display_name FROM users LIMIT 100"))


# ---------------------------------------------------------------- chansons
def add_song(title, artist, album, genre, duration, year):
    sid = uuid.uuid4()
    title, artist, genre = title.strip(), artist.strip(), genre.strip().lower()
    batch(
        ("song_ins", (sid, title, artist, album.strip(), genre, duration, year, now())),
        ("song_artist", (artist, title, sid, album.strip(), genre, duration)),
        ("song_genre", (genre, title, sid, artist, duration)),
        ("song_title", (title.lower(), sid, title, artist, genre)),
    )
    return sid


def search_songs(mode, term):
    term = term.strip()
    if mode == "Titre":
        return list(run("q_title", term.lower()))
    if mode == "Artiste":
        return list(run("q_artist", term))
    return list(run("q_genre", term.lower()))


def catalogue(limit=500):
    return list(get_session().execute(f"SELECT song_id, title, artist, genre, duration_seconds FROM songs LIMIT {limit}"))


# ---------------------------------------------------------------- favoris
def is_favorite(uid, sid):
    return run("fav_look_get", uid, sid).one() is not None


def add_favorite(uid, sid, title, artist):
    ts = now()
    batch(("fav_ins", (uid, ts, sid, title, artist)), ("fav_look_ins", (uid, sid, ts)))


def remove_favorite(uid, sid):
    row = run("fav_look_get", uid, sid).one()
    if row:
        batch(("fav_del", (uid, row.added_at, sid)), ("fav_look_del", (uid, sid)))


# ---------------------------------------------------------------- playlists
def create_playlist(uid, name, description, is_public=False):
    pid = uuid.uuid1()  # timeuuid
    ts = now()
    ops = [
        ("pl_ins", (pid, uid, name.strip(), description, is_public, ts, ts)),
        ("pl_user_ins", (uid, pid, name.strip(), description, is_public, ts)),
    ]
    if is_public:
        ops.append(("pub_ins", (pid, uid, name.strip(), description, ts)))
    batch(*ops)


def set_playlist_public(uid, pid, is_public, name=None, description=None):
    batch(
        ("pl_pub_upd", (is_public, pid)),
        ("pl_user_pub_upd", (is_public, uid, pid)),
        (("pub_ins", (pid, uid, name.strip(), description or "", now())) if is_public else ("pub_del", (pid,))),
    )


def delete_playlist(uid, pid):
    batch(("pl_del", (pid,)), ("pl_user_del", (uid, pid)),
          ("pls_del_all", (pid,)), ("plp_del_all", (pid,)), ("pub_del", (pid,)))


def playlist_songs(pid):
    return list(run("pls_list", pid))


def list_public_playlists():
    return list(run("pub_list"))


def record_listen(uid, song):
    if uid is None or song is None:
        return
    batch(("listen_ins", (uid, now(), song.song_id, song.title, song.artist, getattr(song, "genre", "") or "")))


def list_recent_listens(uid, limit=10):
    return list(run("listen_list", uid))[:limit]


def recommend_for_user(uid, limit=8):
    favs = list(run("fav_list", uid))
    history = list_recent_listens(uid, 20)
    seen = {f.song_id for f in favs}
    seen.update(h.song_id for h in history)
    artist_counts = {}
    for item in favs + history:
        artist = getattr(item, "artist", None)
        if artist:
            artist_counts[artist] = artist_counts.get(artist, 0) + 1
    recs = []
    for artist, _ in sorted(artist_counts.items(), key=lambda kv: kv[1], reverse=True)[:3]:
        for row in list(run("q_artist", artist)):
            if row.song_id not in seen:
                recs.append(row)
                seen.add(row.song_id)
            if len(recs) >= limit:
                return recs
    if not recs:
        for item in history[:5]:
            if not item.genre:
                continue
            for row in list(run("q_genre", item.genre.lower())):
                if row.song_id not in seen:
                    recs.append(row)
                    seen.add(row.song_id)
                if len(recs) >= limit:
                    return recs
    return recs[:limit]


def add_to_playlist(pid, song):
    if run("plp_get", pid, song.song_id).one():
        return False  # déjà présente
    last = run("pls_last", pid).one()
    pos = (last.position + GAP) if last else GAP
    batch(("pls_ins", (pid, pos, song.song_id, song.title, song.artist,
                       getattr(song, "duration_seconds", None), now())),
          ("plp_ins", (pid, song.song_id, pos)))
    return True


def remove_from_playlist(pid, sid):
    row = run("plp_get", pid, sid).one()
    if row:
        batch(("pls_del", (pid, row.position)), ("plp_del", (pid, sid)))


def renumber(pid):
    for i, r in enumerate(playlist_songs(pid), start=1):
        new = i * GAP
        if new != r.position:
            batch(("pls_del", (pid, r.position)),
                  ("pls_ins", (pid, new, r.song_id, r.title, r.artist, r.duration_seconds, r.added_at)),
                  ("plp_ins", (pid, r.song_id, new)))


def move_song(pid, sid, direction, retry=True):
    """direction = -1 (monter) ou +1 (descendre) : position médiane entre les nouveaux voisins."""
    rows = playlist_songs(pid)
    idx = next((i for i, r in enumerate(rows) if r.song_id == sid), None)
    j = None if idx is None else idx + direction
    if j is None or j < 0 or j >= len(rows):
        return
    me = rows[idx]
    others = [r for r in rows if r.song_id != sid]
    before = others[j - 1].position if j > 0 else None
    after = others[j].position if j < len(others) else None
    if before is None:
        p = after // 2
    elif after is None:
        p = before + GAP
    else:
        p = (before + after) // 2
    ok = p > 0 and (before is None or p > before) and (after is None or p < after)
    if not ok:  # plus de place entre deux positions : on renumérote puis on réessaie
        if retry:
            renumber(pid)
            move_song(pid, sid, direction, retry=False)
        return
    batch(("pls_del", (pid, me.position)),
          ("pls_ins", (pid, p, me.song_id, me.title, me.artist, me.duration_seconds, me.added_at)),
          ("plp_ins", (pid, sid, p)))


# ---------------------------------------------------------------- UI helpers
def fmt_duration(sec):
    return f"{sec // 60}:{sec % 60:02d}" if sec else "–"


def song_label(s):
    return f"{s.title} — {s.artist}"


def fav_button(uid, s, key):
    fav = is_favorite(uid, s.song_id)
    if st.button("★" if fav else "☆", key=key, help="Retirer des favoris" if fav else "Ajouter aux favoris"):
        (remove_favorite(uid, s.song_id) if fav else add_favorite(uid, s.song_id, s.title, s.artist))
        st.rerun()


def play_button(uid, s, key):
    if st.button("▶", key=key, help="Enregistrer dans l'historique"):
        record_listen(uid, s)
        st.toast("Écouté")
        st.rerun()


def song_rows(songs, uid, prefix, playlist=None):
    for s in songs:
        c1, c2, c3, c4 = st.columns([5.5, 1, 1, 1])
        c1.markdown(f"**{s.title}** — {s.artist}  \n:gray[{getattr(s, 'genre', '') or ''}]")
        with c2:
            play_button(uid, s, f"{prefix}_play_{s.song_id}")
        with c3:
            fav_button(uid, s, f"{prefix}_fav_{s.song_id}")
        if playlist:
            if c4.button("＋", key=f"{prefix}_add_{s.song_id}", help="Ajouter à la playlist ouverte"):
                st.toast("Ajoutée" if add_to_playlist(playlist, s) else "Déjà dans la playlist")
                st.rerun()


# ---------------------------------------------------------------- pages
def sidebar_login():
    st.sidebar.title("🎵 Playlists")
    users = list_users()
    if users:
        labels = {f"{u.display_name} (@{u.username})": u.user_id for u in users}
        choice = st.sidebar.selectbox("Utilisateur", list(labels), key="user_choice")
        st.session_state["uid"] = labels[choice]
    else:
        st.session_state["uid"] = None
        st.sidebar.info("Aucun utilisateur : créez-en un dans l'onglet Profil.")


def tab_profile():
    uid = st.session_state.get("uid")
    if uid:
        u = run("user_get", uid).one()
        st.subheader("Mon profil")
        st.write(f"**{u.display_name}** (@{u.username})")
        st.write(f"Email : {u.email or '–'} · Pays : {u.country or '–'}")
        st.caption(f"Inscrit le {u.created_at:%d/%m/%Y}")
        st.divider()
        st.subheader("Historique d'écoute")
        history = list_recent_listens(uid)
        if not history:
            st.info("Aucun morceau écouté pour le moment. Utilisez ▶ sur une chanson pour commencer.")
        else:
            for h in history:
                st.markdown(f"- **{h.title}** — {h.artist}  \n:gray[{h.genre or '–'} · {h.listened_at:%d/%m/%Y %H:%M}]")
        st.divider()
    st.subheader("Créer un utilisateur")
    with st.form("new_user", clear_on_submit=True):
        username = st.text_input("Nom d'utilisateur *")
        display = st.text_input("Nom affiché")
        email = st.text_input("Email")
        country = st.text_input("Pays")
        if st.form_submit_button("Créer"):
            if not username.strip():
                st.error("Le nom d'utilisateur est obligatoire.")
            elif create_user(username, email, display, country) is None:
                st.error("Ce nom d'utilisateur existe déjà.")
            else:
                st.success("Utilisateur créé.")
                st.rerun()


def tab_songs(uid):
    st.subheader("Rechercher")
    c1, c2 = st.columns([1, 3])
    mode = c1.radio("Par", ["Titre", "Artiste", "Genre"], horizontal=False)
    term = c2.text_input("Terme de recherche (artiste : respecter la casse exacte)")
    if term:
        results = search_songs(mode, term)
        st.caption(f"{len(results)} résultat(s)")
        song_rows(results, uid, "search", st.session_state.get("open_playlist"))
    st.divider()
    with st.expander("➕ Ajouter une chanson"):
        with st.form("new_song", clear_on_submit=True):
            title = st.text_input("Titre *")
            artist = st.text_input("Artiste *")
            album = st.text_input("Album")
            genre = st.text_input("Genre *")
            duration = st.number_input("Durée (secondes)", 0, 3600, 200)
            year = st.number_input("Année", 1900, 2100, 2020)
            if st.form_submit_button("Ajouter"):
                if not (title.strip() and artist.strip() and genre.strip()):
                    st.error("Titre, artiste et genre sont obligatoires.")
                else:
                    add_song(title, artist, album, genre, int(duration), int(year))
                    st.success("Chanson ajoutée.")
    with st.expander("📚 Catalogue complet"):
        song_rows(catalogue(), uid, "cat", st.session_state.get("open_playlist"))


def tab_playlists(uid):
    with st.expander("➕ Nouvelle playlist"):
        with st.form("new_pl", clear_on_submit=True):
            name = st.text_input("Nom *")
            desc = st.text_area("Description")
            public = st.checkbox("Rendre cette playlist publique")
            if st.form_submit_button("Créer") and name.strip():
                create_playlist(uid, name, desc, public)
                st.rerun()

    public_playlists = list_public_playlists()
    if public_playlists:
        st.subheader("Playlists publiques")
        pub_labels = {f"{p.name} · {p.user_id}" if p.name else str(p.playlist_id): p for p in public_playlists}
        public_choice = st.selectbox("Explorer", list(pub_labels), index=None, placeholder="Choisir une playlist publique...")
        if public_choice:
            chosen = pub_labels[public_choice]
            st.caption(f"{chosen.name} · {chosen.description or 'Aucune description'}")
            public_songs = playlist_songs(chosen.playlist_id)
            if not public_songs:
                st.info("Cette playlist est vide.")
            else:
                song_rows(public_songs, uid, "pub", st.session_state.get("open_playlist"))

    pls = list(run("pl_list", uid))
    if not pls:
        st.info("Aucune playlist pour le moment.")
        st.session_state["open_playlist"] = None
        return
    labels = {p.name: p for p in pls}
    pl = labels[st.selectbox("Mes playlists", list(labels))]
    st.session_state["open_playlist"] = pl.playlist_id
    if pl.description:
        st.caption(pl.description)
    public_toggle = st.checkbox("Playlist publique", value=bool(getattr(pl, "is_public", False)), key=f"pub_toggle_{pl.playlist_id}")
    if public_toggle != bool(getattr(pl, "is_public", False)):
        set_playlist_public(uid, pl.playlist_id, public_toggle, pl.name, pl.description)
        st.rerun()

    songs = playlist_songs(pl.playlist_id)
    total = sum(s.duration_seconds or 0 for s in songs)
    st.write(f"**{len(songs)} morceau(x)** · {fmt_duration(total)}")
    for i, s in enumerate(songs):
        c = st.columns([0.5, 6, 1, 1, 1])
        c[0].write(i + 1)
        c[1].markdown(f"**{s.title}** — {s.artist}  \n:gray[{fmt_duration(s.duration_seconds)}]")
        if c[2].button("↑", key=f"up_{s.song_id}", disabled=i == 0):
            move_song(pl.playlist_id, s.song_id, -1)
            st.rerun()
        if c[3].button("↓", key=f"down_{s.song_id}", disabled=i == len(songs) - 1):
            move_song(pl.playlist_id, s.song_id, +1)
            st.rerun()
        if c[4].button("🗑", key=f"rm_{s.song_id}"):
            remove_from_playlist(pl.playlist_id, s.song_id)
            st.rerun()

    st.divider()
    cat = catalogue()
    if cat:
        opts = {song_label(s): s for s in cat}
        c1, c2 = st.columns([4, 1])
        pick = c1.selectbox("Ajouter une chanson", list(opts), index=None, placeholder="Choisir...")
        if c2.button("Ajouter", disabled=pick is None):
            st.toast("Ajoutée" if add_to_playlist(pl.playlist_id, opts[pick]) else "Déjà dans la playlist")
            st.rerun()
    if st.button("Supprimer cette playlist", type="secondary"):
        delete_playlist(uid, pl.playlist_id)
        st.rerun()


def tab_favorites(uid):
    favs = list(run("fav_list", uid))
    if not favs:
        st.info("Aucun favori. Utilisez ☆ dans la recherche ou le catalogue.")
    for f in favs:
        c1, c2 = st.columns([8, 1])
        c1.markdown(f"**{f.title}** — {f.artist}  \n:gray[ajouté le {f.added_at:%d/%m/%Y %H:%M}]")
        if c2.button("★", key=f"unfav_{f.song_id}", help="Retirer"):
            remove_favorite(uid, f.song_id)
            st.rerun()


def tab_recommendations(uid):
    recs = recommend_for_user(uid)
    if not recs:
        st.info("Ajoute des favoris ou écoute quelques chansons pour générer des recommandations.")
        return
    st.caption("Basé sur vos artistes préférés et votre historique d'écoute.")
    song_rows(recs, uid, "reco", st.session_state.get("open_playlist"))


def main():
    try:
        get_session()
    except Exception as e:
        st.error(f"Impossible de se connecter à Cassandra : {e}")
        st.stop()

    st.markdown(
        """
        <div class="music-header">
            <h1>🎵 Ma musique</h1>
            <p>Gérez vos playlists, favoris, historique et recommandations en une seule vue.</p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    sidebar_login()
    uid = st.session_state.get("uid")
    tabs = st.tabs(["👤 Profil", "🔎 Chansons", "📂 Playlists", "★ Favoris", "🎯 Reco"])
    with tabs[0]:
        tab_profile()
    if uid is None:
        for t in tabs[1:]:
            with t:
                st.info("Créez ou sélectionnez un utilisateur d'abord.")
        return
    with tabs[2]:
        tab_playlists(uid)  # exécuté avant les chansons pour connaître la playlist ouverte
    with tabs[1]:
        tab_songs(uid)
    with tabs[3]:
        tab_favorites(uid)
    with tabs[4]:
        tab_recommendations(uid)


main()
