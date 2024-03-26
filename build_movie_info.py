import datetime

import neo4j
import pandas as pd

import dataset
import neo4j_client


def create_movie_node(s: neo4j.Session, movie):
    s.run(
        """
        MERGE (m:Movie {title: $title, id: $id, release_date: $release_date
            , overview: $overview, vote_average: $vote_average, vote_count: $vote_count})
        """,
        title=movie.title,
        id=int(movie.id),
        release_date=datetime.datetime.strptime(movie.release_date, "%Y-%m-%d").date(),
        overview=movie.overview,
        vote_average=float(movie.vote_average),
        vote_count=int(movie.vote_count),
    )


def create_cast_relationship(s: neo4j.Session, movie):
    for cast in movie.cast:
        s.run(
            "MERGE (p:Person {name: $name, id: $id})",
            name=cast["name"],
            id=int(cast["id"]),
        )
        s.run(
            "MATCH (m:Movie {id: $movie_id}), (p:Person {id: $person_id}) MERGE (p)-[:ACTED_IN {role: $role, order: $order}]->(m)",
            movie_id=int(movie.id),
            person_id=int(cast["id"]),
            role=cast["character"],
            order=int(cast["order"]),
        )


def create_director_relationship(s: neo4j.Session, movie):
    if movie.director:
        s.run(
            "MERGE (p:Person {name: $name, id: $id})",
            name=movie.director["name"],
            id=int(movie.director["id"]),
        )
        s.run(
            "MATCH (m:Movie {id: $movie_id}), (p:Person {id: $person_id}) MERGE (p)-[:DIRECTED]->(m)",
            movie_id=int(movie.id),
            person_id=int(movie.director["id"]),
        )


def build():
    df = pd.merge(dataset.movies_metadata(), dataset.credits(), on="id")

    print("building movie info...")
    with neo4j_client.new_session() as s:
        for _, movie in df.iterrows():
            create_movie_node(s, movie)
            create_cast_relationship(s, movie)
            create_director_relationship(s, movie)


if __name__ == "__main__":
    with neo4j_client.new_session() as s:
        build(s)
