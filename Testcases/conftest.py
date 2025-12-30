"""
Pytest fixtures for FastAPI application testing.

This module provides database setup, authenticated and unauthenticated
test clients, and dependency overrides required for API integration tests.
"""

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from main import app
from db import Base, get_db
from utils.dependencies import get_current_user

# pylint: disable=too-few-public-methods, redefined-builtin
# ---------------- Fake User ----------------
class FakeUser:
    """
    Mock user object used for authenticated test scenarios.
    """
    def __init__(
        self,
        id=1,
        username="test_user",
        email="test@mouritech.com",
    ):
        self.id = id
        self.username = username
        self.email = email


# ---------------- Test Database ----------------
SQLALCHEMY_DATABASE_URL = "sqlite:///./test.db"

engine = create_engine(
    SQLALCHEMY_DATABASE_URL,
    connect_args={"check_same_thread": False},
)

TestingSessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine,
)


# ---------------- Create / Drop Tables ----------------
@pytest.fixture(scope="session", autouse=True)
def create_test_db():
    """
    Create all database tables before test session
    and drop them after tests complete.
    """
    Base.metadata.create_all(bind=engine)
    yield
    Base.metadata.drop_all(bind=engine)


# ---------------- DB Session Fixture ----------------
@pytest.fixture()
def db_session():
    """
    Provide a transactional database session for tests.
    Rolls back after each test.
    """
    db = TestingSessionLocal()
    try:
        yield db
    finally:
        db.rollback()
        db.close()

"""Two fixtures exist because authentication success and failure
require opposite dependency behavior"""
# ---------------- AUTHENTICATED CLIENT ----------------
@pytest.fixture()
def client(db_session):
    """
    Provide an authenticated FastAPI test client.

    Overrides:
    - get_db → test database session
    - get_current_user → FakeUser instance
    """
    def override_get_db():
        yield db_session

    def override_get_current_user():
        return FakeUser()

    app.dependency_overrides[get_db] = override_get_db
    app.dependency_overrides[get_current_user] = override_get_current_user

    with TestClient(app) as client:
        yield client

    app.dependency_overrides.clear()


# # ---------------- UNAUTHENTICATED CLIENT ----------------
@pytest.fixture()
def unauth_client(db_session):
    """
    Provide an unauthenticated FastAPI test client.

    Overrides:
    - get_db → test database session
    """
    def override_get_db():
        yield db_session

    app.dependency_overrides[get_db] = override_get_db

    with TestClient(app) as client:
        yield client

    app.dependency_overrides.clear()
