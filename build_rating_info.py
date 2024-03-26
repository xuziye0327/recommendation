import datetime

import neo4j

import dataset
import neo4j_client


def create_rating_relationship(s: neo4j.Session, rating):
    s.run(
        "MERGE (u:User {id: $id})",
        id=int(rating.userId),
    )

    s.run(
        "MATCH (m:Movie {id: $movie_id}), (u:User {id: $user_id}) MERGE (u)-[:RATED {datetime:$datetime, rating: $rating}]->(m)",
        movie_id=int(rating.movieId),
        user_id=int(rating.userId),
        datetime=datetime.datetime.fromtimestamp(rating.timestamp),
        rating=float(rating.rating),
    )


def _parallel_build(t):
    _, rating = t
    with neo4j_client.new_session() as s:
        create_rating_relationship(s, rating)


def build():
    df = dataset.ratings()

    print("building rating info...")

    import multiprocessing as mp

    mp.Pool().map(_parallel_build, df.iterrows())
