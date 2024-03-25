import ast

import pandas as pd


def _parse(df: pd.DataFrame) -> pd.DataFrame:
    def _literal_eval(x, *args, **argv):
        return ast.literal_eval(x)

    return df.apply(_literal_eval, engine="numba", engine_kwargs={"parallel": True})


class _Dataset:
    def __init__(self):
        self._movies_metadata = pd.DataFrame()
        self._credits = pd.DataFrame()
        self._tags = pd.DataFrame()
        self._ratings = pd.DataFrame()

    def movies_metadata(self) -> pd.DataFrame:
        if not self._movies_metadata.empty:
            return self._movies_metadata

        print("loading movies_metadata...")
        movies_metadata = "archive/movies_metadata.csv"
        df = pd.read_csv(movies_metadata, low_memory=False).dropna()
        df = df[
            ["id", "title", "release_date", "overview", "vote_average", "vote_count"]
        ].astype({"id": int})

        print("loaded movies_metadata")
        self._movies_metadata = df
        return self._movies_metadata

    def credits(self) -> pd.DataFrame:
        if not self._credits.empty:
            return self._credits

        def find_director(x):
            for i in x:
                if i["job"].lower() == "director":
                    return i
            return None

        print("loading credits...")
        credits = "archive/credits.csv"
        df = pd.read_csv(credits, low_memory=False).dropna()
        df = df[["id", "cast", "crew"]].astype({"id": int})
        df["cast"] = _parse(df["cast"])
        df["director"] = _parse(df["crew"]).apply(find_director)
        df.drop(columns=["crew"], inplace=True)

        print("loaded credits")
        self._credits = df
        return self._credits

    def tags(self) -> pd.DataFrame:
        if not self._tags.empty:
            return self._tags

        print("loading tags...")
        keywords = "archive/keywords.csv"
        df = pd.read_csv(keywords, low_memory=False).dropna()
        df["keywords"] = _parse(df["keywords"])
        df = df.astype({"id": int})
        df = pd.merge(self.movies_metadata(), df, on="id")

        print("loaded tags")
        self._tags = df
        return self._tags

    def ratings(self) -> pd.DataFrame:
        if not self._ratings.empty:
            return self._ratings

        print("loading ratings...")
        ratings = "archive/ratings.csv"
        df = pd.read_csv(ratings, low_memory=False).dropna()
        df = df.astype({"userId": int, "movieId": int})
        df["id"] = df["movieId"]
        df = pd.merge(self.movies_metadata(), df, on="id")

        print("loaded ratings")
        self._ratings = df[["userId", "movieId", "rating", "timestamp"]]
        return self._ratings


_dataset = _Dataset()


def movies_metadata() -> pd.DataFrame:
    return _dataset.movies_metadata()


def credits() -> pd.DataFrame:
    return _dataset.credits()


def tags() -> pd.DataFrame:
    return _dataset.tags()


def ratings() -> pd.DataFrame:
    return _dataset.ratings()
