import os

# Superset specific config
ROW_LIMIT = 100000

SQLLAB_TIMEOUT = 300
SUPERSET_TIMEOUT = 300
SQLLAB_ASYNC_TIME_LIMIT_SEC = 300
SUPERSET_WEBSERVER_TIMEOUT = 1000

SUPERSET_WEBSERVER_PORT = 8088

# The SQLAlchemy connection string to your database backend
# This connection defines the path to the database that stores your
# superset metadata (slices, connections, tables, dashboards, ...).
# Note that the connection information to connect to the datasources
# you want to explore are managed directly in the web UI
# SQLALCHEMY_DATABASE_URI = 'sqlite:////app/superset.db'
SQLALCHEMY_DATABASE_URI = os.getenv("SUPERSET_SQL_ALCHEMY")

# Flask-WTF flag for CSRF
WTF_CSRF_ENABLED = False
# Add endpoints that need to be exempt from CSRF protection
WTF_CSRF_EXEMPT_LIST = []
# A CSRF token that expires in 1 year
WTF_CSRF_TIME_LIMIT = 60 * 60 * 24 * 365
