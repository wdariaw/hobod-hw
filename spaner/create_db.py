from google.cloud import spanner
from google.auth.credentials import AnonymousCredentials


def create_database(instance):

    database = instance.database(
        "hw-db2",
        ddl_statements=[
            """CREATE TABLE Movies (
            movieId  INT64 NOT NULL,
            title    STRING(1024),
            genres   ARRAY<STRING(128)>
        ) PRIMARY KEY (movieId)""",
            """CREATE TABLE Ratings (
            userId    INT64 NOT NULL,
            movieId   INT64 NOT NULL,
            rating    FLOAT64,
            timestamp INT64
        ) PRIMARY KEY (userId, movieId)""",
        ],
    )

    operation = database.create()

    print("Waiting for operation to complete...")
    operation.result(OPERATION_TIMEOUT_SECONDS)

    print("Created database")

if __name__ == "__main__":
    client = spanner.Client(
        project='your-project-id',
        client_options={"api_endpoint": "0.0.0.0:9010"},
        credentials=AnonymousCredentials()
    )
    instance = client.instance("test-instance")

    create_database(instance)