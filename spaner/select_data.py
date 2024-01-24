from google.cloud import spanner
from google.auth.credentials import AnonymousCredentials
import time


def select_transaction(database):
    full_result = []
    with database.snapshot(multi_use=True) as snapshot:
        for i in range(10):

            result = snapshot.execute_sql(
                f"SELECT * FROM Ratings LIMIT 10 OFFSET {i * 10}"
            )

            time.sleep(10)

            for x in result:
                full_result.append(x)
    
        with open('res.txt', 'w') as f:
            for line in full_result:
                f.write(f"{line}\n")

    print("Selected data.")


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
    select_transaction(database)
