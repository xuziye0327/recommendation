import datetime

import neo4j

import dataset


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


def build(s: neo4j.Session):
    df = dataset.ratings()

    print("building rating info...")
    for _, rating in df.iterrows():
        create_rating_relationship(s, rating)
