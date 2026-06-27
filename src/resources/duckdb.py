from dagster_duckdb import DuckDBResource


def get_duckdb_resource(db_path: str = "output/helix.duckdb") -> DuckDBResource:
    return DuckDBResource(database=db_path)
