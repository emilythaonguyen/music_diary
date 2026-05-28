--
-- PostgreSQL database dump
--

-- Dumped from database version 14.13 (Homebrew)
-- Dumped by pg_dump version 14.13 (Homebrew)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: pg_trgm; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS pg_trgm WITH SCHEMA public;


--
-- Name: EXTENSION pg_trgm; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION pg_trgm IS 'text similarity measurement and index searching based on trigrams';


--
-- Name: artist_type_enum; Type: TYPE; Schema: public; Owner: emilynguyen
--

CREATE TYPE public.artist_type_enum AS ENUM (
    'Person',
    'Group',
    'Other',
    'Orchestra',
    'Choir',
    'Character'
);


ALTER TYPE public.artist_type_enum OWNER TO emilynguyen;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: artist_aliases; Type: TABLE; Schema: public; Owner: emilynguyen
--

CREATE TABLE public.artist_aliases (
    alias_id integer NOT NULL,
    artist_id integer NOT NULL,
    alias_name character varying(500) NOT NULL,
    source character varying(500),
    locale character varying(500),
    primary_alias boolean DEFAULT false,
    added_at timestamp without time zone DEFAULT now(),
    retrieved_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


ALTER TABLE public.artist_aliases OWNER TO emilynguyen;

--
-- Name: artist_aliases_alias_id_seq; Type: SEQUENCE; Schema: public; Owner: emilynguyen
--

CREATE SEQUENCE public.artist_aliases_alias_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.artist_aliases_alias_id_seq OWNER TO emilynguyen;

--
-- Name: artist_aliases_alias_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: emilynguyen
--

ALTER SEQUENCE public.artist_aliases_alias_id_seq OWNED BY public.artist_aliases.alias_id;


--
-- Name: artist_genres; Type: TABLE; Schema: public; Owner: emilynguyen
--

CREATE TABLE public.artist_genres (
    artist_id integer NOT NULL,
    genre_id integer NOT NULL,
    retrieved_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


ALTER TABLE public.artist_genres OWNER TO emilynguyen;

--
-- Name: artists; Type: TABLE; Schema: public; Owner: emilynguyen
--

CREATE TABLE public.artists (
    artist_id integer NOT NULL,
    artist_name character varying(500) NOT NULL,
    sort_name character varying(500) NOT NULL,
    artist_mbid uuid,
    spotify_id character varying(100),
    artist_type public.artist_type_enum NOT NULL,
    start_date date,
    end_date date,
    country character varying(500),
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    on_mb boolean,
    on_spotify boolean,
    retrieved_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    isni character varying(16)
);


ALTER TABLE public.artists OWNER TO emilynguyen;

--
-- Name: artists_artist_id_seq; Type: SEQUENCE; Schema: public; Owner: emilynguyen
--

CREATE SEQUENCE public.artists_artist_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.artists_artist_id_seq OWNER TO emilynguyen;

--
-- Name: artists_artist_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: emilynguyen
--

ALTER SEQUENCE public.artists_artist_id_seq OWNED BY public.artists.artist_id;


--
-- Name: genre_hierarchy; Type: TABLE; Schema: public; Owner: emilynguyen
--

CREATE TABLE public.genre_hierarchy (
    hierarchy_id integer NOT NULL,
    child_genre_id integer NOT NULL,
    parent_genre_id integer NOT NULL,
    depth integer DEFAULT 1 NOT NULL,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


ALTER TABLE public.genre_hierarchy OWNER TO emilynguyen;

--
-- Name: genre_hierarchy_hierarchy_id_seq; Type: SEQUENCE; Schema: public; Owner: emilynguyen
--

CREATE SEQUENCE public.genre_hierarchy_hierarchy_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.genre_hierarchy_hierarchy_id_seq OWNER TO emilynguyen;

--
-- Name: genre_hierarchy_hierarchy_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: emilynguyen
--

ALTER SEQUENCE public.genre_hierarchy_hierarchy_id_seq OWNED BY public.genre_hierarchy.hierarchy_id;


--
-- Name: genres; Type: TABLE; Schema: public; Owner: emilynguyen
--

CREATE TABLE public.genres (
    genre_id integer NOT NULL,
    genre_name character varying(100) NOT NULL,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    wikidata_id character varying(20) NOT NULL,
    description text,
    retrieved_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


ALTER TABLE public.genres OWNER TO emilynguyen;

--
-- Name: genres_genre_id_seq; Type: SEQUENCE; Schema: public; Owner: emilynguyen
--

CREATE SEQUENCE public.genres_genre_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.genres_genre_id_seq OWNER TO emilynguyen;

--
-- Name: genres_genre_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: emilynguyen
--

ALTER SEQUENCE public.genres_genre_id_seq OWNED BY public.genres.genre_id;


--
-- Name: listens; Type: TABLE; Schema: public; Owner: emilynguyen
--

CREATE TABLE public.listens (
    listen_id integer NOT NULL,
    listened_at timestamp without time zone NOT NULL,
    track_id integer,
    release_id integer,
    artist_id integer,
    device character varying(50),
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


ALTER TABLE public.listens OWNER TO emilynguyen;

--
-- Name: listens_listen_id_seq; Type: SEQUENCE; Schema: public; Owner: emilynguyen
--

CREATE SEQUENCE public.listens_listen_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.listens_listen_id_seq OWNER TO emilynguyen;

--
-- Name: listens_listen_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: emilynguyen
--

ALTER SEQUENCE public.listens_listen_id_seq OWNED BY public.listens.listen_id;


--
-- Name: ratings; Type: TABLE; Schema: public; Owner: emilynguyen
--

CREATE TABLE public.ratings (
    rating_id integer NOT NULL,
    release_id integer,
    rating numeric(3,2),
    date_rated date,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


ALTER TABLE public.ratings OWNER TO emilynguyen;

--
-- Name: ratings_rating_id_seq; Type: SEQUENCE; Schema: public; Owner: emilynguyen
--

CREATE SEQUENCE public.ratings_rating_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.ratings_rating_id_seq OWNER TO emilynguyen;

--
-- Name: ratings_rating_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: emilynguyen
--

ALTER SEQUENCE public.ratings_rating_id_seq OWNED BY public.ratings.rating_id;


--
-- Name: release_artists; Type: TABLE; Schema: public; Owner: emilynguyen
--

CREATE TABLE public.release_artists (
    release_id integer NOT NULL,
    artist_id integer NOT NULL,
    retrieved_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


ALTER TABLE public.release_artists OWNER TO emilynguyen;

--
-- Name: release_genres; Type: TABLE; Schema: public; Owner: emilynguyen
--

CREATE TABLE public.release_genres (
    release_id integer NOT NULL,
    genre_id integer NOT NULL,
    retrieved_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


ALTER TABLE public.release_genres OWNER TO emilynguyen;

--
-- Name: releases; Type: TABLE; Schema: public; Owner: emilynguyen
--

CREATE TABLE public.releases (
    release_id integer NOT NULL,
    release_name character varying(500) NOT NULL,
    release_mbid uuid,
    release_type character varying(50),
    release_date date,
    language character varying(50),
    num_tracks integer,
    length_ms integer,
    spotify_id character varying(100),
    primary_artist_id integer,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    release_group_mbid uuid,
    on_mb boolean,
    on_spotify boolean,
    retrieved_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    upc character varying(15)
);


ALTER TABLE public.releases OWNER TO emilynguyen;

--
-- Name: releases_release_id_seq; Type: SEQUENCE; Schema: public; Owner: emilynguyen
--

CREATE SEQUENCE public.releases_release_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.releases_release_id_seq OWNER TO emilynguyen;

--
-- Name: releases_release_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: emilynguyen
--

ALTER SEQUENCE public.releases_release_id_seq OWNED BY public.releases.release_id;


--
-- Name: spotify_artist_popularity; Type: TABLE; Schema: public; Owner: emilynguyen
--

CREATE TABLE public.spotify_artist_popularity (
    spotify_pop_id integer NOT NULL,
    artist_id integer,
    popularity integer,
    snapshot_date date,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


ALTER TABLE public.spotify_artist_popularity OWNER TO emilynguyen;

--
-- Name: spotify_artist_popularity_spotify_pop_id_seq; Type: SEQUENCE; Schema: public; Owner: emilynguyen
--

CREATE SEQUENCE public.spotify_artist_popularity_spotify_pop_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.spotify_artist_popularity_spotify_pop_id_seq OWNER TO emilynguyen;

--
-- Name: spotify_artist_popularity_spotify_pop_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: emilynguyen
--

ALTER SEQUENCE public.spotify_artist_popularity_spotify_pop_id_seq OWNED BY public.spotify_artist_popularity.spotify_pop_id;


--
-- Name: spotify_release_popularity; Type: TABLE; Schema: public; Owner: emilynguyen
--

CREATE TABLE public.spotify_release_popularity (
    spotify_pop_id integer NOT NULL,
    release_id integer,
    popularity integer,
    snapshot_date date,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


ALTER TABLE public.spotify_release_popularity OWNER TO emilynguyen;

--
-- Name: spotify_release_popularity_spotify_pop_id_seq; Type: SEQUENCE; Schema: public; Owner: emilynguyen
--

CREATE SEQUENCE public.spotify_release_popularity_spotify_pop_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.spotify_release_popularity_spotify_pop_id_seq OWNER TO emilynguyen;

--
-- Name: spotify_release_popularity_spotify_pop_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: emilynguyen
--

ALTER SEQUENCE public.spotify_release_popularity_spotify_pop_id_seq OWNED BY public.spotify_release_popularity.spotify_pop_id;


--
-- Name: spotify_track_popularity; Type: TABLE; Schema: public; Owner: emilynguyen
--

CREATE TABLE public.spotify_track_popularity (
    spotify_pop_id integer NOT NULL,
    track_id integer,
    popularity integer,
    snapshot_date date,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


ALTER TABLE public.spotify_track_popularity OWNER TO emilynguyen;

--
-- Name: spotify_track_popularity_spotify_pop_id_seq; Type: SEQUENCE; Schema: public; Owner: emilynguyen
--

CREATE SEQUENCE public.spotify_track_popularity_spotify_pop_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.spotify_track_popularity_spotify_pop_id_seq OWNER TO emilynguyen;

--
-- Name: spotify_track_popularity_spotify_pop_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: emilynguyen
--

ALTER SEQUENCE public.spotify_track_popularity_spotify_pop_id_seq OWNED BY public.spotify_track_popularity.spotify_pop_id;


--
-- Name: track_artists; Type: TABLE; Schema: public; Owner: emilynguyen
--

CREATE TABLE public.track_artists (
    track_id integer NOT NULL,
    artist_id integer NOT NULL,
    retrieved_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


ALTER TABLE public.track_artists OWNER TO emilynguyen;

--
-- Name: track_audio_features; Type: TABLE; Schema: public; Owner: emilynguyen
--

CREATE TABLE public.track_audio_features (
    track_id integer NOT NULL,
    danceability numeric(4,3),
    bpm numeric(6,2),
    loudness numeric(5,2),
    key text,
    scale text,
    retrieved_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    dynamic_complexity numeric(5,2),
    chords_complexity numeric(5,2),
    tuning_frequency numeric(6,2)
);


ALTER TABLE public.track_audio_features OWNER TO emilynguyen;

--
-- Name: tracks; Type: TABLE; Schema: public; Owner: emilynguyen
--

CREATE TABLE public.tracks (
    track_id integer NOT NULL,
    track_name character varying(500) NOT NULL,
    recording_mbid uuid,
    duration_ms integer,
    track_number integer,
    release_id integer,
    spotify_id character varying(100),
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    on_mb boolean,
    on_spotify boolean,
    retrieved_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    isrc character varying(12)
);


ALTER TABLE public.tracks OWNER TO emilynguyen;

--
-- Name: tracks_track_id_seq; Type: SEQUENCE; Schema: public; Owner: emilynguyen
--

CREATE SEQUENCE public.tracks_track_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.tracks_track_id_seq OWNER TO emilynguyen;

--
-- Name: tracks_track_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: emilynguyen
--

ALTER SEQUENCE public.tracks_track_id_seq OWNED BY public.tracks.track_id;


--
-- Name: artist_aliases alias_id; Type: DEFAULT; Schema: public; Owner: emilynguyen
--

ALTER TABLE ONLY public.artist_aliases ALTER COLUMN alias_id SET DEFAULT nextval('public.artist_aliases_alias_id_seq'::regclass);


--
-- Name: artists artist_id; Type: DEFAULT; Schema: public; Owner: emilynguyen
--

ALTER TABLE ONLY public.artists ALTER COLUMN artist_id SET DEFAULT nextval('public.artists_artist_id_seq'::regclass);


--
-- Name: genre_hierarchy hierarchy_id; Type: DEFAULT; Schema: public; Owner: emilynguyen
--

ALTER TABLE ONLY public.genre_hierarchy ALTER COLUMN hierarchy_id SET DEFAULT nextval('public.genre_hierarchy_hierarchy_id_seq'::regclass);


--
-- Name: genres genre_id; Type: DEFAULT; Schema: public; Owner: emilynguyen
--

ALTER TABLE ONLY public.genres ALTER COLUMN genre_id SET DEFAULT nextval('public.genres_genre_id_seq'::regclass);


--
-- Name: listens listen_id; Type: DEFAULT; Schema: public; Owner: emilynguyen
--

ALTER TABLE ONLY public.listens ALTER COLUMN listen_id SET DEFAULT nextval('public.listens_listen_id_seq'::regclass);


--
-- Name: ratings rating_id; Type: DEFAULT; Schema: public; Owner: emilynguyen
--

ALTER TABLE ONLY public.ratings ALTER COLUMN rating_id SET DEFAULT nextval('public.ratings_rating_id_seq'::regclass);


--
-- Name: releases release_id; Type: DEFAULT; Schema: public; Owner: emilynguyen
--

ALTER TABLE ONLY public.releases ALTER COLUMN release_id SET DEFAULT nextval('public.releases_release_id_seq'::regclass);


--
-- Name: spotify_artist_popularity spotify_pop_id; Type: DEFAULT; Schema: public; Owner: emilynguyen
--

ALTER TABLE ONLY public.spotify_artist_popularity ALTER COLUMN spotify_pop_id SET DEFAULT nextval('public.spotify_artist_popularity_spotify_pop_id_seq'::regclass);


--
-- Name: spotify_release_popularity spotify_pop_id; Type: DEFAULT; Schema: public; Owner: emilynguyen
--

ALTER TABLE ONLY public.spotify_release_popularity ALTER COLUMN spotify_pop_id SET DEFAULT nextval('public.spotify_release_popularity_spotify_pop_id_seq'::regclass);


--
-- Name: spotify_track_popularity spotify_pop_id; Type: DEFAULT; Schema: public; Owner: emilynguyen
--

ALTER TABLE ONLY public.spotify_track_popularity ALTER COLUMN spotify_pop_id SET DEFAULT nextval('public.spotify_track_popularity_spotify_pop_id_seq'::regclass);


--
-- Name: tracks track_id; Type: DEFAULT; Schema: public; Owner: emilynguyen
--

ALTER TABLE ONLY public.tracks ALTER COLUMN track_id SET DEFAULT nextval('public.tracks_track_id_seq'::regclass);


--
-- Name: artist_aliases artist_aliases_pkey; Type: CONSTRAINT; Schema: public; Owner: emilynguyen
--

ALTER TABLE ONLY public.artist_aliases
    ADD CONSTRAINT artist_aliases_pkey PRIMARY KEY (alias_id);


--
-- Name: artist_genres artist_genres_pkey; Type: CONSTRAINT; Schema: public; Owner: emilynguyen
--

ALTER TABLE ONLY public.artist_genres
    ADD CONSTRAINT artist_genres_pkey PRIMARY KEY (artist_id, genre_id);


--
-- Name: artists artists_artist_mbid_unique; Type: CONSTRAINT; Schema: public; Owner: emilynguyen
--

ALTER TABLE ONLY public.artists
    ADD CONSTRAINT artists_artist_mbid_unique UNIQUE (artist_mbid);


--
-- Name: artists artists_isni_key; Type: CONSTRAINT; Schema: public; Owner: emilynguyen
--

ALTER TABLE ONLY public.artists
    ADD CONSTRAINT artists_isni_key UNIQUE (isni);


--
-- Name: artists artists_pkey; Type: CONSTRAINT; Schema: public; Owner: emilynguyen
--

ALTER TABLE ONLY public.artists
    ADD CONSTRAINT artists_pkey PRIMARY KEY (artist_id);


--
-- Name: genre_hierarchy genre_hierarchy_child_genre_id_parent_genre_id_key; Type: CONSTRAINT; Schema: public; Owner: emilynguyen
--

ALTER TABLE ONLY public.genre_hierarchy
    ADD CONSTRAINT genre_hierarchy_child_genre_id_parent_genre_id_key UNIQUE (child_genre_id, parent_genre_id);


--
-- Name: genre_hierarchy genre_hierarchy_pkey; Type: CONSTRAINT; Schema: public; Owner: emilynguyen
--

ALTER TABLE ONLY public.genre_hierarchy
    ADD CONSTRAINT genre_hierarchy_pkey PRIMARY KEY (hierarchy_id);


--
-- Name: genres genres_pkey; Type: CONSTRAINT; Schema: public; Owner: emilynguyen
--

ALTER TABLE ONLY public.genres
    ADD CONSTRAINT genres_pkey PRIMARY KEY (genre_id);


--
-- Name: genres genres_wikidata_id_key; Type: CONSTRAINT; Schema: public; Owner: emilynguyen
--

ALTER TABLE ONLY public.genres
    ADD CONSTRAINT genres_wikidata_id_key UNIQUE (wikidata_id);


--
-- Name: listens listens_pkey; Type: CONSTRAINT; Schema: public; Owner: emilynguyen
--

ALTER TABLE ONLY public.listens
    ADD CONSTRAINT listens_pkey PRIMARY KEY (listen_id);


--
-- Name: ratings ratings_pkey; Type: CONSTRAINT; Schema: public; Owner: emilynguyen
--

ALTER TABLE ONLY public.ratings
    ADD CONSTRAINT ratings_pkey PRIMARY KEY (rating_id);


--
-- Name: ratings ratings_release_id_key; Type: CONSTRAINT; Schema: public; Owner: emilynguyen
--

ALTER TABLE ONLY public.ratings
    ADD CONSTRAINT ratings_release_id_key UNIQUE (release_id);


--
-- Name: release_artists release_artists_pkey; Type: CONSTRAINT; Schema: public; Owner: emilynguyen
--

ALTER TABLE ONLY public.release_artists
    ADD CONSTRAINT release_artists_pkey PRIMARY KEY (release_id, artist_id);


--
-- Name: release_artists release_artists_unique; Type: CONSTRAINT; Schema: public; Owner: emilynguyen
--

ALTER TABLE ONLY public.release_artists
    ADD CONSTRAINT release_artists_unique UNIQUE (release_id, artist_id);


--
-- Name: release_genres release_genres_pkey; Type: CONSTRAINT; Schema: public; Owner: emilynguyen
--

ALTER TABLE ONLY public.release_genres
    ADD CONSTRAINT release_genres_pkey PRIMARY KEY (release_id, genre_id);


--
-- Name: releases releases_pkey; Type: CONSTRAINT; Schema: public; Owner: emilynguyen
--

ALTER TABLE ONLY public.releases
    ADD CONSTRAINT releases_pkey PRIMARY KEY (release_id);


--
-- Name: releases releases_upc_key; Type: CONSTRAINT; Schema: public; Owner: emilynguyen
--

ALTER TABLE ONLY public.releases
    ADD CONSTRAINT releases_upc_key UNIQUE (upc);


--
-- Name: spotify_artist_popularity spotify_artist_popularity_pkey; Type: CONSTRAINT; Schema: public; Owner: emilynguyen
--

ALTER TABLE ONLY public.spotify_artist_popularity
    ADD CONSTRAINT spotify_artist_popularity_pkey PRIMARY KEY (spotify_pop_id);


--
-- Name: spotify_release_popularity spotify_release_popularity_pkey; Type: CONSTRAINT; Schema: public; Owner: emilynguyen
--

ALTER TABLE ONLY public.spotify_release_popularity
    ADD CONSTRAINT spotify_release_popularity_pkey PRIMARY KEY (spotify_pop_id);


--
-- Name: spotify_track_popularity spotify_track_popularity_pkey; Type: CONSTRAINT; Schema: public; Owner: emilynguyen
--

ALTER TABLE ONLY public.spotify_track_popularity
    ADD CONSTRAINT spotify_track_popularity_pkey PRIMARY KEY (spotify_pop_id);


--
-- Name: track_artists track_artists_pkey; Type: CONSTRAINT; Schema: public; Owner: emilynguyen
--

ALTER TABLE ONLY public.track_artists
    ADD CONSTRAINT track_artists_pkey PRIMARY KEY (track_id, artist_id);


--
-- Name: track_artists track_artists_unique; Type: CONSTRAINT; Schema: public; Owner: emilynguyen
--

ALTER TABLE ONLY public.track_artists
    ADD CONSTRAINT track_artists_unique UNIQUE (track_id, artist_id);


--
-- Name: track_audio_features track_audio_features_pkey; Type: CONSTRAINT; Schema: public; Owner: emilynguyen
--

ALTER TABLE ONLY public.track_audio_features
    ADD CONSTRAINT track_audio_features_pkey PRIMARY KEY (track_id);


--
-- Name: tracks tracks_pkey; Type: CONSTRAINT; Schema: public; Owner: emilynguyen
--

ALTER TABLE ONLY public.tracks
    ADD CONSTRAINT tracks_pkey PRIMARY KEY (track_id);


--
-- Name: artist_aliases unique_artist_alias; Type: CONSTRAINT; Schema: public; Owner: emilynguyen
--

ALTER TABLE ONLY public.artist_aliases
    ADD CONSTRAINT unique_artist_alias UNIQUE (artist_id, alias_name);


--
-- Name: tracks unique_isrc; Type: CONSTRAINT; Schema: public; Owner: emilynguyen
--

ALTER TABLE ONLY public.tracks
    ADD CONSTRAINT unique_isrc UNIQUE (isrc);


--
-- Name: listens uq_listen; Type: CONSTRAINT; Schema: public; Owner: emilynguyen
--

ALTER TABLE ONLY public.listens
    ADD CONSTRAINT uq_listen UNIQUE (track_id, listened_at);


--
-- Name: idx_artist_genres_artist; Type: INDEX; Schema: public; Owner: emilynguyen
--

CREATE INDEX idx_artist_genres_artist ON public.artist_genres USING btree (artist_id);


--
-- Name: idx_artist_genres_genre; Type: INDEX; Schema: public; Owner: emilynguyen
--

CREATE INDEX idx_artist_genres_genre ON public.artist_genres USING btree (genre_id);


--
-- Name: idx_genre_hierarchy_child; Type: INDEX; Schema: public; Owner: emilynguyen
--

CREATE INDEX idx_genre_hierarchy_child ON public.genre_hierarchy USING btree (child_genre_id);


--
-- Name: idx_genre_hierarchy_parent; Type: INDEX; Schema: public; Owner: emilynguyen
--

CREATE INDEX idx_genre_hierarchy_parent ON public.genre_hierarchy USING btree (parent_genre_id);


--
-- Name: idx_genres_wikidata; Type: INDEX; Schema: public; Owner: emilynguyen
--

CREATE INDEX idx_genres_wikidata ON public.genres USING btree (wikidata_id);


--
-- Name: idx_listens_artist; Type: INDEX; Schema: public; Owner: emilynguyen
--

CREATE INDEX idx_listens_artist ON public.listens USING btree (artist_id);


--
-- Name: idx_listens_release; Type: INDEX; Schema: public; Owner: emilynguyen
--

CREATE INDEX idx_listens_release ON public.listens USING btree (release_id);


--
-- Name: idx_listens_timestamp; Type: INDEX; Schema: public; Owner: emilynguyen
--

CREATE INDEX idx_listens_timestamp ON public.listens USING btree (listened_at);


--
-- Name: idx_listens_track; Type: INDEX; Schema: public; Owner: emilynguyen
--

CREATE INDEX idx_listens_track ON public.listens USING btree (track_id);


--
-- Name: idx_release_genres_genre; Type: INDEX; Schema: public; Owner: emilynguyen
--

CREATE INDEX idx_release_genres_genre ON public.release_genres USING btree (genre_id);


--
-- Name: idx_release_genres_release; Type: INDEX; Schema: public; Owner: emilynguyen
--

CREATE INDEX idx_release_genres_release ON public.release_genres USING btree (release_id);


--
-- Name: idx_releases_artist; Type: INDEX; Schema: public; Owner: emilynguyen
--

CREATE INDEX idx_releases_artist ON public.releases USING btree (primary_artist_id);


--
-- Name: idx_tracks_release; Type: INDEX; Schema: public; Owner: emilynguyen
--

CREATE INDEX idx_tracks_release ON public.tracks USING btree (release_id);


--
-- Name: uq_artist_mbid_not_null; Type: INDEX; Schema: public; Owner: emilynguyen
--

CREATE UNIQUE INDEX uq_artist_mbid_not_null ON public.artists USING btree (artist_mbid) WHERE (artist_mbid IS NOT NULL);


--
-- Name: uq_release_mbid_not_null; Type: INDEX; Schema: public; Owner: emilynguyen
--

CREATE UNIQUE INDEX uq_release_mbid_not_null ON public.releases USING btree (release_mbid) WHERE (release_mbid IS NOT NULL);


--
-- Name: artist_aliases artist_aliases_artist_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: emilynguyen
--

ALTER TABLE ONLY public.artist_aliases
    ADD CONSTRAINT artist_aliases_artist_id_fkey FOREIGN KEY (artist_id) REFERENCES public.artists(artist_id) ON DELETE CASCADE;


--
-- Name: artist_genres artist_genres_artist_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: emilynguyen
--

ALTER TABLE ONLY public.artist_genres
    ADD CONSTRAINT artist_genres_artist_id_fkey FOREIGN KEY (artist_id) REFERENCES public.artists(artist_id);


--
-- Name: artist_genres artist_genres_genre_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: emilynguyen
--

ALTER TABLE ONLY public.artist_genres
    ADD CONSTRAINT artist_genres_genre_id_fkey FOREIGN KEY (genre_id) REFERENCES public.genres(genre_id);


--
-- Name: listens fk_listens_artist; Type: FK CONSTRAINT; Schema: public; Owner: emilynguyen
--

ALTER TABLE ONLY public.listens
    ADD CONSTRAINT fk_listens_artist FOREIGN KEY (artist_id) REFERENCES public.artists(artist_id) ON DELETE RESTRICT;


--
-- Name: listens fk_listens_release; Type: FK CONSTRAINT; Schema: public; Owner: emilynguyen
--

ALTER TABLE ONLY public.listens
    ADD CONSTRAINT fk_listens_release FOREIGN KEY (release_id) REFERENCES public.releases(release_id) ON DELETE RESTRICT;


--
-- Name: listens fk_listens_track; Type: FK CONSTRAINT; Schema: public; Owner: emilynguyen
--

ALTER TABLE ONLY public.listens
    ADD CONSTRAINT fk_listens_track FOREIGN KEY (track_id) REFERENCES public.tracks(track_id) ON DELETE RESTRICT;


--
-- Name: release_artists fk_ra_artist; Type: FK CONSTRAINT; Schema: public; Owner: emilynguyen
--

ALTER TABLE ONLY public.release_artists
    ADD CONSTRAINT fk_ra_artist FOREIGN KEY (artist_id) REFERENCES public.artists(artist_id) ON DELETE RESTRICT;


--
-- Name: releases fk_releases_primary_artist; Type: FK CONSTRAINT; Schema: public; Owner: emilynguyen
--

ALTER TABLE ONLY public.releases
    ADD CONSTRAINT fk_releases_primary_artist FOREIGN KEY (primary_artist_id) REFERENCES public.artists(artist_id) ON DELETE RESTRICT;


--
-- Name: track_artists fk_ta_artist; Type: FK CONSTRAINT; Schema: public; Owner: emilynguyen
--

ALTER TABLE ONLY public.track_artists
    ADD CONSTRAINT fk_ta_artist FOREIGN KEY (artist_id) REFERENCES public.artists(artist_id) ON DELETE RESTRICT;


--
-- Name: tracks fk_tracks_release; Type: FK CONSTRAINT; Schema: public; Owner: emilynguyen
--

ALTER TABLE ONLY public.tracks
    ADD CONSTRAINT fk_tracks_release FOREIGN KEY (release_id) REFERENCES public.releases(release_id) ON DELETE RESTRICT;


--
-- Name: genre_hierarchy genre_hierarchy_child_genre_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: emilynguyen
--

ALTER TABLE ONLY public.genre_hierarchy
    ADD CONSTRAINT genre_hierarchy_child_genre_id_fkey FOREIGN KEY (child_genre_id) REFERENCES public.genres(genre_id) ON DELETE CASCADE;


--
-- Name: genre_hierarchy genre_hierarchy_parent_genre_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: emilynguyen
--

ALTER TABLE ONLY public.genre_hierarchy
    ADD CONSTRAINT genre_hierarchy_parent_genre_id_fkey FOREIGN KEY (parent_genre_id) REFERENCES public.genres(genre_id) ON DELETE CASCADE;


--
-- Name: listens listens_release_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: emilynguyen
--

ALTER TABLE ONLY public.listens
    ADD CONSTRAINT listens_release_id_fkey FOREIGN KEY (release_id) REFERENCES public.releases(release_id);


--
-- Name: listens listens_track_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: emilynguyen
--

ALTER TABLE ONLY public.listens
    ADD CONSTRAINT listens_track_id_fkey FOREIGN KEY (track_id) REFERENCES public.tracks(track_id);


--
-- Name: ratings ratings_release_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: emilynguyen
--

ALTER TABLE ONLY public.ratings
    ADD CONSTRAINT ratings_release_id_fkey FOREIGN KEY (release_id) REFERENCES public.releases(release_id);


--
-- Name: release_artists release_artists_artist_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: emilynguyen
--

ALTER TABLE ONLY public.release_artists
    ADD CONSTRAINT release_artists_artist_id_fkey FOREIGN KEY (artist_id) REFERENCES public.artists(artist_id);


--
-- Name: release_artists release_artists_release_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: emilynguyen
--

ALTER TABLE ONLY public.release_artists
    ADD CONSTRAINT release_artists_release_id_fkey FOREIGN KEY (release_id) REFERENCES public.releases(release_id);


--
-- Name: release_genres release_genres_genre_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: emilynguyen
--

ALTER TABLE ONLY public.release_genres
    ADD CONSTRAINT release_genres_genre_id_fkey FOREIGN KEY (genre_id) REFERENCES public.genres(genre_id);


--
-- Name: release_genres release_genres_release_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: emilynguyen
--

ALTER TABLE ONLY public.release_genres
    ADD CONSTRAINT release_genres_release_id_fkey FOREIGN KEY (release_id) REFERENCES public.releases(release_id);


--
-- Name: spotify_artist_popularity spotify_artist_popularity_artist_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: emilynguyen
--

ALTER TABLE ONLY public.spotify_artist_popularity
    ADD CONSTRAINT spotify_artist_popularity_artist_id_fkey FOREIGN KEY (artist_id) REFERENCES public.artists(artist_id);


--
-- Name: spotify_release_popularity spotify_release_popularity_release_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: emilynguyen
--

ALTER TABLE ONLY public.spotify_release_popularity
    ADD CONSTRAINT spotify_release_popularity_release_id_fkey FOREIGN KEY (release_id) REFERENCES public.releases(release_id);


--
-- Name: spotify_track_popularity spotify_track_popularity_track_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: emilynguyen
--

ALTER TABLE ONLY public.spotify_track_popularity
    ADD CONSTRAINT spotify_track_popularity_track_id_fkey FOREIGN KEY (track_id) REFERENCES public.tracks(track_id);


--
-- Name: track_artists track_artists_artist_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: emilynguyen
--

ALTER TABLE ONLY public.track_artists
    ADD CONSTRAINT track_artists_artist_id_fkey FOREIGN KEY (artist_id) REFERENCES public.artists(artist_id);


--
-- Name: track_artists track_artists_track_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: emilynguyen
--

ALTER TABLE ONLY public.track_artists
    ADD CONSTRAINT track_artists_track_id_fkey FOREIGN KEY (track_id) REFERENCES public.tracks(track_id);


--
-- Name: track_audio_features track_audio_features_track_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: emilynguyen
--

ALTER TABLE ONLY public.track_audio_features
    ADD CONSTRAINT track_audio_features_track_id_fkey FOREIGN KEY (track_id) REFERENCES public.tracks(track_id);


--
-- Name: tracks tracks_release_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: emilynguyen
--

ALTER TABLE ONLY public.tracks
    ADD CONSTRAINT tracks_release_id_fkey FOREIGN KEY (release_id) REFERENCES public.releases(release_id);


--
-- PostgreSQL database dump complete
--

