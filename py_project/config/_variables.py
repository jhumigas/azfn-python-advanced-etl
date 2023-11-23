import logging
from dataclasses import dataclass
from typing import Callable, Optional

from decouple import config
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


@dataclass(frozen=True)
class Env:
    """Loads all environment variables into a predefined set of properties"""

    default = None
    env_code: Optional[str] = str(config("ENV", "l")).lower()
    db_host: Optional[str] = config("DB_HOST", "db_host")
    db_port: Optional[str] = config("DB_PORT", 5432)
    db_name: Optional[str] = config("DB_NAME", "db_name")
    db_user: Optional[str] = config("DB_USER", "db_user")

    if env_code == "l":
        _db_password: Optional[str] = config("DB_PASSWORD", "db_password")
        connection_string: str = f"postgresql+psycopg2://{db_user}:{_db_password}@{db_host}:{db_port}/{db_name}"
    else:
        import testing.postgresql

        postgresql = testing.postgresql.Postgresql()
        connection_string = postgresql.url()
    engine: Engine = create_engine(connection_string, pool_size=10, max_overflow=20)

    def check_not_none(property_getter: Callable) -> Callable:
        def wrapper(*args, **kwargs):
            value = property_getter(*args, **kwargs)
            if value is None:
                logging.warning(f"{property_getter.__name__} is None")
            return value

        return wrapper

    @property
    @check_not_none
    def db_password(self) -> str:
        return self._db_password

    check_not_none = staticmethod(check_not_none)
