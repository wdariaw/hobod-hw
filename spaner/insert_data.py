from google.cloud import spanner
from google.auth.credentials import AnonymousCredentials
from google.protobuf import field_mask_pb2  # type: ignore

import pandas as pd


ratings = pd.read_csv(
    "ratings.csv", 
    sep=",", 
    header=0,
    index_col=None,
    dtype={"userId": int, "movieId": int, "rating": float, "timestamp": int})

n_rows = len(ratings)
batch_size = 10000


def insert_ratings(database):
    for i in range(n_rows // batch_size):
        with database.batch() as batch:
            batch.insert(
                table="Ratings",
                columns=("userId", "movieId", "rating", "timestamp"),
                values=list(map(list, ratings.iloc[i * batch_size:(i + 1) * batch_size].itertuples(index=False))),
            )
        
        if i % 10 == 0:
            print(f"Inserted {i * batch_size} rows, rows in table: {n_rows}")

    with database.batch() as batch:
        batch.insert(
            table="Ratings",
            columns=("userId", "movieId", "rating", "timestamp"),
            values=list(map(list, ratings.iloc[(n_rows // batch_size) * batch_size:].itertuples(index=False)))
        )

    print("Inserted data.")


movies = pd.read_csv(
    "movies.csv", 
    sep=",", 
    header=0,
    index_col=None,
    dtype={"movieId": int, "title": str, "genres": str})


def insert_movies(database):
    data = []
    for _, row in movies.iterrows():
        data.append(
            [
                row["movieId"],
                row["title"],
                row["genres"].split('|')
            ]
        )
    with database.batch() as batch:
        batch.insert(
            table="Movies",
            columns=("movieId", "title", "genres"),
            values=data
        )



if __name__ == "__main__":
    client = spanner.Client(
        project='your-project-id',
        client_options={"api_endpoint": "0.0.0.0:9010"},
        credentials=AnonymousCredentials()
    )
    instance = client.instance("test-instance")
    database = instance.database(
        "hw-db2"
    )
    insert_ratings(database)
    insert_movies(database)
