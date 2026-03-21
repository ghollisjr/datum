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

# Conditionally install Oracle Instant Client + ODBC driver
ARG DATUM_INCLUDE_ORACLE=1
RUN if [ "$DATUM_INCLUDE_ORACLE" = "1" ]; then \
    apt-get update && apt-get install -y --no-install-recommends alien libaio1 wget && \
    wget -q https://download.oracle.com/otn_software/linux/instantclient/2340000/oracle-instantclient-basic-23.4.0.24.05-1.el9.x86_64.rpm && \
    wget -q https://download.oracle.com/otn_software/linux/instantclient/2340000/oracle-instantclient-odbc-23.4.0.24.05-1.el9.x86_64.rpm && \
    alien -i oracle-instantclient-basic-*.rpm && \
    alien -i oracle-instantclient-odbc-*.rpm && \
    rm -f *.rpm && \
    apt-get purge -y alien wget && apt-get autoremove -y && \
    rm -rf /var/lib/apt/lists/* && \
    echo "[Oracle 23 ODBC driver]" >> /etc/odbcinst.ini && \
    echo "Description = Oracle ODBC driver for Oracle 23" >> /etc/odbcinst.ini && \
    echo "Driver = /usr/lib/oracle/23/client64/lib/libsqora.so.23.1" >> /etc/odbcinst.ini && \
    echo "Setup =" >> /etc/odbcinst.ini && \
    echo "FileUsage = 1" >> /etc/odbcinst.ini ; \
    fi

WORKDIR /app

# Install datum and test dependencies
COPY setup.cfg pyproject.toml ./
COPY datum/ ./datum/
RUN pip install --no-cache-dir -e . && \
    pip install --no-cache-dir pytest

CMD ["bash", "tests/run-all.sh"]
