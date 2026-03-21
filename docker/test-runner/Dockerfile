FROM python:3.12-slim-bookworm

# Prevent interactive prompts during package install
ENV DEBIAN_FRONTEND=noninteractive

# Install unixODBC, SQLite, and base tools
RUN apt-get update && apt-get install -y --no-install-recommends \
    unixodbc \
    unixodbc-dev \
    odbc-postgresql \
    odbc-mariadb \
    libsqliteodbc \
    sqlite3 \
    curl \
    gnupg2 \
    apt-transport-https \
    && rm -rf /var/lib/apt/lists/*

# Install Microsoft ODBC Driver 18 for SQL Server
RUN curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | \
    gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg && \
    echo "deb [arch=amd64 signed-by=/usr/share/keyrings/microsoft-prod.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" > \
    /etc/apt/sources.list.d/mssql-release.list && \
    apt-get update && \
    ACCEPT_EULA=Y apt-get install -y --no-install-recommends msodbcsql18 && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install datum and test dependencies
COPY setup.cfg pyproject.toml ./
COPY datum/ ./datum/
RUN pip install --no-cache-dir -e . && \
    pip install --no-cache-dir pytest

CMD ["bash", "tests/run-all.sh"]
