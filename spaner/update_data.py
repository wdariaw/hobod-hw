from google.cloud import spanner
from google.auth.credentials import AnonymousCredentials


def update_transaction(transaction):
    res = transaction.execute_update(
        "UPDATE Ratings "
        "SET rating = rating + 2 "
        "WHERE movieId in (SELECT movieId FROM Movies where ARRAY_INCLUDES(genres, 'Fantasy'))"
    )

    print(res)


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
    database.run_in_transaction(update_transaction)
