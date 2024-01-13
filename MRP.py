import os
import sqlite3
import random

random.seed(38)


if os.path.exists('MRP_database.db'):
    os.remove('MRP_database.db')



def create_tables():
    conn = sqlite3.connect('MRP_database.db')
    cursor = conn.cursor()

    # Period table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Period (
            PeriodID INTEGER PRIMARY KEY
        )
    ''')

    # Part table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Part (
            PartID INTEGER PRIMARY KEY,
            LeadTime INTEGER,
            InitialInventory INTEGER,
            LotSize INTEGER,
            MakeOrBuy TEXT,
            BOMLevel INTEGER
        )
    ''')

    # BOM table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS BOM (
            PartID INTEGER,
            ComponentPartID INTEGER,
            Multiplier INTEGER,
            Level INTEGER,
            PRIMARY KEY (PartID, ComponentPartID),
            FOREIGN KEY (PartID) REFERENCES Part(PartID),
            FOREIGN KEY (ComponentPartID) REFERENCES Part(PartID)
        )
    ''')

    # MRP table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS MRP (
            PartID INTEGER,
            PeriodID INTEGER,
            GrossRequirements INTEGER,
            ScheduledReceipts INTEGER,
            EndingInventory INTEGER,
            NetRequirements INTEGER,
            PlannedOrderRelease INTEGER,
            PlannedOrderReceipts INTEGER,
            PRIMARY KEY (PartID, PeriodID),
            FOREIGN KEY (PartID) REFERENCES Part(PartID),
            FOREIGN KEY (PeriodID) REFERENCES Period(PeriodID)
        )
    ''')

    conn.commit()
    conn.close()


def insert_sample_data():
    conn = sqlite3.connect('MRP_database.db')
    cursor = conn.cursor()

    for period_id in range(1, 20):
        cursor.execute("INSERT OR IGNORE INTO Period (PeriodID) VALUES (?)", (period_id,))

    # Insert sample data into the Part table
    for values in [
        (1, 2, 10, 100, 'Make', 0),  # A
        (2, 3, 20, 250, 'Make', 1),  # B
        (3, 2, 10, 100, 'Make', 1),  # C
        (4, 2, 20, 200, 'Make', 2),  # D
        (5, 3, 30, 300, 'Buy', 3),  # E
        (6, 1, 20, 200, 'Buy', 3),  # F
        (7, 4, 10, 300, 'Buy', 3)  # G
    ]:
        try:
            cursor.execute(
                "INSERT INTO Part (PartID, LeadTime, InitialInventory, LotSize, MakeOrBuy, BOMLevel) VALUES (?, ?, ?, ?, ?, ?)",
                values)
        except sqlite3.IntegrityError:
            # If the same PartID already exists, update the existing record
            cursor.execute(
                "UPDATE Part SET LeadTime=?, InitialInventory=?, LotSize=?, MakeOrBuy=?, BOMLevel=? WHERE PartID=?",
                values[1:] + (values[0],))

    # Insert sample data into the BOM table
    for values in [
        (1, 2, 1, 0),  # A requires 1 B
        (1, 3, 3, 0),  # A requires 3 C
        (2, 4, 1, 1),  # B requires 1 D
        (2, 3, 2, 1),  # B requires 2 C
        (3, 5, 3, 1),  # C requires 3 E
        (4, 5, 1, 2),  # D requires 1 E
        (4, 6, 1, 2),  # D requires 1 F
        (4, 7, 2, 2)   # D requires 2 G
    ]:
        try:
            cursor.execute("INSERT INTO BOM (PartID, ComponentPartID, Multiplier, Level) VALUES (?, ?, ?, ?)", values)
        except sqlite3.IntegrityError:
            # If the same (PartID, ComponentPartID) combination already exists, update the existing record
            cursor.execute("UPDATE BOM SET Multiplier=?, Level=? WHERE PartID=? AND ComponentPartID=?",
                           values[2:] + values[:2])

    conn.commit()
    conn.close()

def generate_random_gross_requirements_for_part_a():
    # Generate random Gross Requirements between 30 and 60 for each period for the main product we produce, which has Level 0 in Bill of Materials
    return [(period_id, random.randint(30, 60)) for period_id in range(1, 20)]


def insert_gross_requirements_for_part_a(gross_requirements):
    conn = sqlite3.connect('MRP_database.db')
    cursor = conn.cursor()

    try:
        for values in gross_requirements:
            cursor.execute(
                "INSERT INTO MRP (PartID, PeriodID, GrossRequirements) VALUES (?, ?, ?)",
                (1, values[0], values[1])
            )

    finally:
        # Commit changes and close connection
        conn.commit()
        conn.close()


def update_gross_requirements_based_on_bom(part_id):
    conn = sqlite3.connect('MRP_database.db')
    cursor = conn.cursor()

    try:
        # Get the BOM components for the given part
        cursor.execute("SELECT PartID, ComponentPartID, Multiplier FROM BOM WHERE ComponentPartID = ?", (part_id,))
        bom_components = cursor.fetchall()

        for part_id, component_part_id, multiplier in bom_components:
            lead_time = cursor.execute("SELECT LeadTime FROM Part WHERE PartID = ?", (component_part_id,)).fetchone()[0]

            for current_period in range(1, 20):
                cursor.execute("""
                    INSERT OR REPLACE INTO MRP (PartID, PeriodID, GrossRequirements)
                    SELECT
                        ? AS PartID,
                        ? AS PeriodID,
                        COALESCE(
                            (
                                SELECT GrossRequirements * BOM.Multiplier
                                FROM MRP
                                WHERE PartID = PartID
                                  AND PeriodID = ? + PART.LeadTime
                            ),
                            0
                        ) AS GrossRequirements
                    FROM BOM
                    JOIN PART ON BOM.ComponentPartID = PART.PartID
                    WHERE BOM.PartID = ?;
                """, (component_part_id, current_period, current_period, part_id))

                conn.commit()


    # Rare error but happened before
    except sqlite3.IntegrityError as e:
        print(f"IntegrityError: {e}")

    finally:
        conn.close()


def calculate_mrp_values_for_part(part_id):
    conn = sqlite3.connect('MRP_database.db')
    cursor = conn.cursor()

    # Set values for period 1

    cursor.execute("""
        UPDATE MRP
        SET
            NetRequirements = 0,
            ScheduledReceipts = CASE
                WHEN GrossRequirements > COALESCE((SELECT InitialInventory FROM Part WHERE PartID = ?), 0) AND
                     COALESCE((SELECT LotSize FROM Part WHERE PartID = ?), 0) IS NOT NULL THEN
                    COALESCE((SELECT LotSize FROM Part WHERE PartID = ?), 0)
                ELSE 0
            END,
            PlannedOrderReceipts = 0,
            PlannedOrderRelease = 0
        WHERE PartID = ? AND PeriodID = 1;
    """, (part_id, part_id, part_id, part_id))

    cursor.execute("""
        UPDATE MRP
        SET
            EndingInventory = COALESCE((SELECT InitialInventory FROM Part WHERE PartID = ?), 0) -
                              GrossRequirements +
                              COALESCE(ScheduledReceipts, 0)
        WHERE PartID = ? AND PeriodID = 1;
    """, (part_id, part_id))

    # Iterate through periods other than 1
    for current_period in range(2, 20):  # Update the range as needed
        cursor.execute("""
                UPDATE MRP
                SET ScheduledReceipts = 0,
                    PlannedOrderRelease = 0,
                    NetRequirements = CASE
                        WHEN GrossRequirements > (
                            SELECT EndingInventory FROM MRP WHERE PartID = ? AND PeriodID = ?
                        ) THEN GrossRequirements - (
                            SELECT EndingInventory FROM MRP WHERE PartID = ? AND PeriodID = ?
                        )
                        ELSE 0
                    END
                WHERE PartID = ? AND PeriodID = ?;
            """, (part_id, current_period - 1, part_id, current_period - 1, part_id, current_period))

        cursor.execute("""
            UPDATE MRP
            SET
                PlannedOrderReceipts = CASE
                    WHEN (
                        SELECT EndingInventory FROM MRP WHERE PartID = ? AND PeriodID = ?
                    ) - GrossRequirements < 0 THEN (
                        SELECT LotSize FROM Part WHERE PartID = ?
                    )
                    ELSE 0
                END
            WHERE PartID = ? AND PeriodID = ?;
        """, (part_id, current_period - 1, part_id, part_id, current_period))

        cursor.execute("""
                UPDATE MRP
                SET EndingInventory = CASE
                    WHEN GrossRequirements > (
                        SELECT EndingInventory FROM MRP WHERE PartID = ? AND PeriodID = ?
                    ) THEN (
                        SELECT LotSize FROM Part WHERE PartID = ?
                    ) - NetRequirements
                    ELSE (
                        SELECT EndingInventory FROM MRP WHERE PartID = ? AND PeriodID = ?
                    ) - GrossRequirements
                END
                WHERE PartID = ? AND PeriodID = ?;
            """, (part_id, current_period - 1, part_id, part_id, current_period - 1, part_id, current_period))

        lead_time = cursor.execute("SELECT LeadTime FROM Part WHERE PartID = ?", (part_id,)).fetchone()[0]
        if current_period - lead_time > 0:
            cursor.execute("""
                    UPDATE MRP
                    SET PlannedOrderRelease = (
                        SELECT LotSize FROM Part WHERE PartID = ? AND PeriodID = ?
                    )
                    WHERE PartID = ? AND PeriodID = ?;
                """, (part_id, current_period - lead_time, part_id, current_period))

        cursor.execute("""
            UPDATE MRP
            SET PlannedOrderRelease = (
                CASE
                    WHEN (
                        SELECT PlannedOrderReceipts FROM MRP
                        WHERE PartID = ? AND PeriodID = ?
                    ) > 0 AND ? - (
                        SELECT LeadTime FROM Part WHERE PartID = ?
                    ) > 0 THEN (
                        SELECT LotSize FROM Part WHERE PartID = ?
                    )
                    ELSE 0
                END
            )
            WHERE PartID = ? AND PeriodID = ?;
        """, (part_id, current_period, current_period, part_id, part_id, part_id, current_period - lead_time))

    conn.commit()
    conn.close()


def calculate_mrp_values_for_periods():
    # Calculate MRP values for all parts
    conn = sqlite3.connect('MRP_database.db')
    cursor = conn.cursor()

    cursor.execute("""
           SELECT PartID
           FROM Part
           ORDER BY BomLevel
       """)
    part_ids = cursor.fetchall()
    for part_id_tuple in part_ids:
        part_id = part_id_tuple[0]  # Extract the part_id from the tuple

        bom_level = cursor.execute("SELECT MAX(Level) FROM BOM WHERE PartID = ?", (part_id,)).fetchone()[0]

        if bom_level != 0:
            update_gross_requirements_based_on_bom(part_id)
            calculate_mrp_values_for_part(part_id)
        else:
            gross_requirements_part_a = generate_random_gross_requirements_for_part_a()
            insert_gross_requirements_for_part_a(gross_requirements_part_a)
            calculate_mrp_values_for_part(part_id)

    conn.commit()
    conn.close()

"""
from tabulate import tabulate

def print_mrp_for_part_a():
    conn = sqlite3.connect('MRP_database.db')
    cursor = conn.cursor()

    try:
        cursor.execute("SELECT * FROM MRP WHERE PartID = 7 ORDER BY PeriodID")
        mrp_results = cursor.fetchall()

        headers = ["PeriodID", "GrossRequirements", "ScheduledReceipts", "EndingInventory",
                   "NetRequirements", "PlannedOrderRelease", "PlannedOrderReceipts"]
        print(tabulate(mrp_results, headers=headers, tablefmt="grid"))

    finally:
        conn.close()
"""

def test_mrp():
    conn = sqlite3.connect('MRP_database.db')
    cursor = conn.cursor()

    try:
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        if not tables:
            create_tables()
        insert_sample_data()
        calculate_mrp_values_for_periods()
        #print_mrp_for_part_a()


    finally:
        conn.close()

if __name__ == "__main__":
    create_tables()
    insert_sample_data()
    test_mrp()
