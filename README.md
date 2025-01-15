# django-ldb-mover

`django-ldb-mover` is a command-line tool designed to facilitate the migration of local SQLite3 databases to various remote databases, such as PostgreSQL and MySQL, during development primarily in Django

## Features
- **Supports Multiple Databases**: Easily migrate data to PostgreSQL, MySQL, and more.
- **JSON Export/Import**: Converts the local SQLite3 database to a JSON file for flexible data handling.

## Installation
1. Clone the repository:

    ```bash
    git clone https://github.com/yourusername/django-ldb-mover.git
    ```

2. Navigate to the project directory:

    ```bash
    cd django-ldb-mover
    ```

3. Install the required dependencies:

    ```bash
    pip install -r requirements.txt
    ```

## Usage

To move data from an SQLite3 database to a target database (e.g., PostgreSQL), use the following command:

```bash
ldb_mover -move -postgres -connections.txt database.sqlite3
