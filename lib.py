import os
from datetime import datetime
from pathlib import Path
from rich.console import Console
from duckdb import DuckDBPyConnection, connect

console = Console()


def initialize_connection(database: str = ":memory:") -> DuckDBPyConnection:
    """
    Initializes a database (in-memory by default) by a database value.

    Parameters:
        database (str): A name/URL of database. Defaults to ':memory:'

    Returns:
        DuckDBPyConnection: An SQL connection ready-to-work
    """
    conn = connect(database, read_only=False)

    # Install and load extension for spatial information
    conn.install_extension("spatial")
    conn.load_extension("spatial")

    console.log("[green on default]Successfully initialized database.[/]", markup=True)

    return conn


def create_enum_from_column(
    csv_file_dir: str, col_name: str, enum_name: str = ""
) -> str:
    """
    Creates a enum SQL command for database from CSV file by the column name.

    Parameters:
        csv_file_dir (str): A source directory of the CSV file.
        col_name (str): A name of the column that should transform to enum.
        enum_name (str, optional): A custom name of enum. Defaults to empty string.

    Returns:
        str: An SQL script that should be executed
    """

    if len(enum_name) == 0:
        enum_name = col_name.replace(" ", "").replace("(", "").replace(")", "")

    return f"""
        CREATE TYPE {enum_name} as ENUM (
            FROM read_csv('{csv_file_dir}', AUTO_DETECT=true)
            SELECT DISTINCT "{col_name}"
            WHERE "{col_name}" IS NOT NULL
        );
    """


def create_table_from_csv(csv_file_dir: Path, conn: DuckDBPyConnection) -> str:
    """
    Initializes a database table from CSV file. Returns table name.

    Parameters:
        csv_file_dir (str): A source directory of the CSV file.
        conn (DuckDBPyConnection): A connection instance of DuckDB database.

    Returns:
        str: A name of the table where data is stores.
    """

    csv_posix_dir = csv_file_dir.as_posix()
    create_electric_vehicle_enum_command = create_enum_from_column(
        csv_posix_dir, "Electric Vehicle Type"
    )
    create_cafv_eligibility_enum_command = create_enum_from_column(
        csv_posix_dir,
        "Clean Alternative Fuel Vehicle (CAFV) Eligibility",
        "CAFVEligibility",
    )

    table_name = csv_file_dir.stem.lower()
    create_table_command = f"""
        CREATE TABLE {table_name} (
            vin VARCHAR(10) NOT NULL,
            county VARCHAR(40) NOT NULL,
            city VARCHAR(40) NOT NULL,
            state VARCHAR(2) NOT NULL,
            postal_code CHAR(5) NOT NULL,
            model_year INTEGER NOT NULL,
            -- that could be enum, but amount of brands
            -- is not constant (SQL perspective, more general)
            -- however, it would be performant for this scheme 
            -- to make it enum (task perspective, more narrow)
            make VARCHAR(30) NOT NULL,
            model VARCHAR(30),
            -- extracted to enum type
            electric_vehicle_type ElectricVehicleType NOT NULL,
            -- extracted to enum type
            cafv_eligibility CAFVEligibility NOT NULL,
            electric_range INTEGER NOT NULL,
            base_msrp INTEGER NOT NULL,
            legislative_district INTEGER,
            dol_vehicle_id VARCHAR(9) NOT NULL,
            vehicle_location GEOMETRY,
            electric_utility VARCHAR(200),
            census_tract_2000 CHAR(11) NOT NULL
        );
    """

    copy_table_command = f"""
        COPY {table_name} FROM '{csv_posix_dir}'
        WITH (HEADER 1, DELIMITER ',');
    """

    console.log(
        f"[bold][yellow on default]Creating table and importing data from [underline]{csv_posix_dir}[/]...[/]",
        markup=True,
    )
    # Create enums first
    conn.execute(create_electric_vehicle_enum_command)
    conn.execute(create_cafv_eligibility_enum_command)

    # Initialize table
    conn.execute(create_table_command)

    # Copy data from CSV
    conn.execute(copy_table_command)
    console.log(
        f"[green on default]Successfully imported data into created table {table_name}.[/]",
        markup=True,
    )

    return table_name


def collect_information(
    table_name: str, conn: DuckDBPyConnection, output_dir: str = ""
) -> None:
    """
    Collects desired information and exports it into Parquet files.

    Parameters:
        table_name (str): A name of table to get data from.
        conn (DuckDBPyConnection): A connection instance of DuckDB database.
        output_dir (str, optional): A custom path into which data should be exported. Generates by default.
    """

    if len(output_dir) == 0:
        output_dir = os.path.join(
            "output", datetime.today().strftime("%Y-%m-%d_%H-%M-%S")
        )

    console.log("[bold][yellow on default]Collecting data...[/]", markup=True)

    os.makedirs(output_dir, exist_ok=True)

    # Amount of vehicles in every city
    conn.execute(
        f"""
        COPY (
            SELECT
                city,
                COUNT(*) as electric_vehicle_amount
            FROM {table_name}
            GROUP BY city
        )
        TO '{output_dir}/vehicles_per_city.parquet' (FORMAT 'PARQUET');
    """
    )

    # Three most popular vehicles
    conn.execute(
        f"""
        COPY (
            SELECT
                make,
                model, 
                COUNT(*) as vehicle_amount
            FROM {table_name}
            GROUP BY make, model
            ORDER BY vehicle_amount
            DESC
            LIMIT 3
        )
        TO '{output_dir}/three_most_popular_vehicles.parquet' (FORMAT 'PARQUET');
    """
    )

    # The most popular vehicle per postal code
    conn.execute(
        f"""
        COPY (
            SELECT
                postal_code,
                make,
                model,
                vehicle_count
            FROM (
                SELECT
                    postal_code,
                    make,
                    model,
                    COUNT(*) as vehicle_count,
                    ROW_NUMBER() OVER (PARTITION BY postal_code ORDER BY COUNT(*) DESC) as row_num
                FROM {table_name}
                GROUP BY postal_code, make, model
            ) AS ranked
            WHERE row_num = 1
            ORDER BY vehicle_count DESC
        )
        TO '{output_dir}/most_popular_vehicle_per_postal_code.parquet' (FORMAT 'PARQUET');
    """
    )

    # Amount of vehicles per model year
    conn.execute(
        f"""
        COPY (
            SELECT
                model_year,
                COUNT(*) as vehicle_amount
            FROM {table_name}
            GROUP BY model_year
        )
        TO '{output_dir}/per_year'
        (
            FORMAT 'PARQUET',
            PARTITION_BY(model_year) 
        );
    """
    )

    console.log(
        f"[green on default]Results are exported into [bold][underline]{output_dir}.[/][/]",
        markup=True,
    )
