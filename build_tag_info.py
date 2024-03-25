import neo4j

import dataset


def build_tag_relationship(s: neo4j.Session, movie):
    for tag in movie.keywords:
        s.run(
            "MERGE (t:Tag {name: $name, id: $id})",
            name=tag["name"],
            id=int(tag["id"]),
        )

        s.run(
            "MATCH (m:Movie {id: $movie_id}), (t:Tag {id: $tag_id}) MERGE (t)-[:IS_TAG_OF]->(m)",
            movie_id=int(movie.id),
            tag_id=int(tag["id"]),
        )


def build(s: neo4j.Session):
    df = dataset.tags()

    print("building tag info...")
    for _, movie in df.iterrows():
        build_tag_relationship(s, movie)
