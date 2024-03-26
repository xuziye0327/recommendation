import build_movie_info
import build_rating_info
import build_tag_info


def main():
    build_movie_info.build()
    build_tag_info.build()
    build_rating_info.build()


if __name__ == "__main__":
    main()
