from pathlib import Path

from lib import collect_information, create_table_from_csv, initialize_connection

def main():
    csv_file_dir = Path("data/Electric_Vehicle_Population_Data.csv")

    # Open connection to create a table
    conn = initialize_connection()
    
    # Import CSV file into database
    table = create_table_from_csv(csv_file_dir, conn)

    # Collect all required data
    collect_information(table, conn)

    conn.close()


if __name__ == "__main__":
    main()
