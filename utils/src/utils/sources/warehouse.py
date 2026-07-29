import dagster as dg
from sqlalchemy import create_engine, URL
from sqlalchemy.orm import Session

class WarehouseResource(dg.ConfigurableResource):
    """ Resource for connecting to the data warehouse. """
    def get_session(self) -> Session:
        """Creates a SQLAlchemy session for the data warehouse."""
        # Construct the database URL
        url = URL.create(
            drivername="postgresql+psycopg2",
            username=dg.EnvVar("DW_USER").get_value(),
            password=dg.EnvVar("DW_PASSWORD").get_value(),
            host=dg.EnvVar("DW_HOST").get_value(),
            port=dg.EnvVar("DW_PORT").get_value(5432),
            database=dg.EnvVar("DW_DATABASE").get_value()
        )
        # Create the engine and session
        engine = create_engine(url)
        return Session(engine)
