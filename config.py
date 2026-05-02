import os

# Redshift connection (uses Data API, not direct SQL)
REDSHIFT_CLUSTER = "instawork-dw"
REDSHIFT_DATABASE = "instawork"
REDSHIFT_REGION = "us-west-2"
# The DbtAccess SSO role has redshift:GetClusterCredentials scoped to this
# specific dbuser (same one the awslabs MCP server uses).
REDSHIFT_DB_USER = os.environ.get("REDSHIFT_DB_USER", "cursor_analytics")

# Flask — accept either SECRET_KEY or SESSION_SECRET (Replit's default name)
SECRET_KEY = (
    os.environ.get("SECRET_KEY")
    or os.environ.get("SESSION_SECRET")
    or "dev-secret-key-change-in-prod"
)
UPLOAD_FOLDER = "/tmp/retail-shift-uploads"
