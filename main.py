import build_movie_info
import build_rating_info
import build_tag_info
import neo4j_client


def main():
    with neo4j_client.new_session() as s:
        build_movie_info.build(s)
        build_tag_info.build(s)
        build_rating_info.build(s)


if __name__ == "__main__":
    main()
