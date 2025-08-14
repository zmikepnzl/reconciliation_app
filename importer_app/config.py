import os

# The only configuration needed is the database connection string.
# The Docker environment provides this.
DATABASE_URL = os.environ.get("DATABASE_URL")

# These variables are still used by the app logic to identify the correct database names.
# In the future, you could remove these and hardcode the names if you prefer.
WORKSPACE_NAME = os.environ.get("WORKSPACE_NAME")
DATABASE_NAME = os.environ.get("DATABASE_NAME")
