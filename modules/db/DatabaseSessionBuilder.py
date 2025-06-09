import pandas as pd
from sqlalchemy import create_engine


class DatabaseSessionBuilder:
    def __init__(self, db_url):
        """Initialize the session builder with a database URL."""
        self.engine = create_engine(db_url)
        self.connection = self.engine.connect()


    def write_data(self, table_name, df, if_exists="replace"):
        """Write a DataFrame to a database table."""
        df.to_sql(table_name, con=self.engine, if_exists=if_exists, index=False)
        print(f"Data written to table '{table_name}' successfully!")

    def query_data(self, query):
        """Fetch data using an SQL query."""
        return pd.read_sql(query, con=self.engine)

    def close_session(self):
        """Close the database connection."""
        self.connection.close()
        print("Database session closed.")


