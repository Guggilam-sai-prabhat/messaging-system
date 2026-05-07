from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    pass


VALID_ROLES = {"owner", "admin", "member"}
