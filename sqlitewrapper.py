
"""
Jarrod Burns, 2022

Table names are scrubbed for every input. This is likely overkill however, I
would like the leave the option open for the user to name tables.
"""


import sqlite3


class SQLiteWrapper:
    """
    Connections are opened and closed per transaction.
    """

    def __init__(self, db_name):
        self.db_name = db_name
        self.con = sqlite3.connect(self.db_name)
        self.handle = self.con.cursor()

    def close(self):
        self.con.commit()
        self.handle.close()

    def scrubbed(self, table_name: str) -> bool:
        """
        Returns false if table_name is an empty string or
        contains disallowed characters.
        Allowed characters are alphanumerics and underscores (A-z, 0-9, or _ )
        """

        if table_name:
            for letter in table_name:
                if not letter.isalnum() and letter != "_":
                    print(
                        'ERROR: Table names may only contain alphanumeric '
                        'characters or underscores.\n'
                        '       Check your input and try again.'
                    )
                    return False
            return True
        else:
            print("ERROR: No name given.\n"
                  "       Check your input and try again.")
            return False

    def drop_table(self,
                   table_name: str,
                   print_destruction_message: bool = True) -> None:

        if not self.scrubbed(table_name):
            return

        drop_statment = """DROP TABLE IF EXISTS {}""".format(table_name)

        self.handle.execute(drop_statment)
        self.close()

        if print_destruction_message:
            print(table_name.lower().title().replace("_", " "),
                  "deleted successfully.")

    def create_table(self,
                     table_name: str,
                     print_creation_message: bool = True) -> None:

        if not self.scrubbed(table_name):
            return

        creation_statment = """CREATE TABLE IF NOT EXISTS {}
                               (NAME VARCHAR(200) NOT NULL,
                               DESCRIPTION TEXT,
                               UNIQUE(NAME, DESCRIPTION))""".format(table_name)

        self.handle.execute(creation_statment)
        self.close()

        if print_creation_message:
            print(f'"{table_name.lower().title().replace("_", " ")}" '
                  "created successfully.")

    def empty_table(self, table_name: str) -> bool:
        """
        Queries the specified table and returns the number of rows available.
        """
        if not self.scrubbed(table_name):
            return

        select_statement = """SELECT count(*) FROM {}""".format(table_name)

        self.handle.execute(select_statement)
        row_count = self.handle.fetchone()
        self.close()

        if not row_count[0]:
            return True
        return False

    def insert_row(self,
                   table_name: str,
                   insert_values: tuple[str, str]) -> None:

        if not self.scrubbed(table_name):
            return

        insert_statment = """INSERT INTO {}
                             (NAME, DESCRIPTION)
                             VALUES (?, ?)""".format(table_name)

        try:
            self.handle.execute(insert_statment, insert_values)
            self.close()
            print("Record created successfully.")

        except sqlite3.IntegrityError:
            self.close()
            print(f'ERROR: "{insert_values[0]}" entry could not be created '
                  f'in {table_name.lower().title().replace("_", " ")}\n'
                  "       All name entries must be unique.")

    def delete_row_byid(self, table_name: str, row_id: str) -> None:

        if not self.scrubbed(table_name):
            return

        delete_statement = """DELETE FROM {}
                              WHERE ROWID = (?)""".format(table_name)

        self.handle.execute(delete_statement, str(row_id))
        self.close()

    def all_table_rows(self, table_name: str) -> list[tuple[str, str], ...]:

        if not self.scrubbed(table_name):
            return

        select_statement = """SELECT * FROM {}""".format(table_name)

        self.handle.execute(select_statement)
        all_rows = self.handle.fetchall()
        self.close()

        return all_rows

    def random_row(self, table_name: str) -> tuple[int, str, str]:

        if not self.scrubbed(table_name):
            return

        select_statement = """SELECT ROWID, * from {}
                              ORDER BY RANDOM()
                              LIMIT 1""".format(table_name)

        self.handle.execute(select_statement)
        selected_row = self.handle.fetchone()
        self.close()

        return selected_row


def main():
    pass


if __name__ == "__main__":
    main()
