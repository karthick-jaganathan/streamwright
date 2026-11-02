import datetime
import os
import sys
import uuid

import pytest

# inside the StreamWright repository: its streamwright/ folder would shadow the installed streamwright packages
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", ".."))
sys.path[:] = [p for p in sys.path if os.path.abspath(p or os.curdir) != REPO_ROOT]

URI = os.environ.get("STREAMWRIGHT_TEST_MONGODB_URI")


@pytest.fixture
def mongo():
    """(database, uri) of a throwaway database on STREAMWRIGHT_TEST_MONGODB_URI with a `users` collection."""
    if not URI:
        pytest.skip("set STREAMWRIGHT_TEST_MONGODB_URI to a throwaway MongoDB")
    pymongo = pytest.importorskip("pymongo")
    bson = pytest.importorskip("bson")
    database = "mg_db_%s" % uuid.uuid4().hex[:10]
    with pymongo.MongoClient(URI) as server:
        server[database].users.insert_many([
            {"_id": bson.ObjectId("65f000000000000000000001"), "name": "Ada", "status": "active",
             "since": datetime.datetime(2026, 10, 5, 8, 0), "spend": bson.Decimal128("12.50")},
            {"_id": bson.ObjectId("65f000000000000000000002"), "name": "Bob", "status": "inactive", "spend": None},
            {"_id": bson.ObjectId("65f000000000000000000003"), "name": "Cy", "status": "active"}])
        yield database, URI.rstrip("/") + "/" + database
        server.drop_database(database)
