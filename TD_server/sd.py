import smokedduck

class SDClient:
    def __init__(self):
        # Initialize a smokedduck connection
        self.con = smokedduck.prov_connect(database=':memory:', read_only=False)
        self._initial_sql()

    def _initial_sql(self):
        self.con.execute("""
        CREATE SCHEMA IF NOT EXISTS internal;
        """)
        self.con.execute("""
        CREATE SEQUENCE internal.seq_user_id START 1;
        """)
        self.con.execute("""
        CREATE TABLE IF NOT EXISTS internal.user (
            user_id INTEGER PRIMARY KEY DEFAULT nextval('internal.seq_user_id'),
            username TEXT UNIQUE,
            admin BOOLEAN,
            password TEXT
        );
        """)
        self.con.execute("""
        CREATE SEQUENCE internal.seq_log_id START 1;
        """)
        self.con.execute("""
        CREATE TABLE IF NOT EXISTS internal.query_log (
            log_id INTEGER PRIMARY KEY DEFAULT nextval('internal.seq_log_id'),
            query_id INTEGER NOT NULL,
            query_text TEXT NOT NULL,
            query_plan TEXT NOT NULL,
            executed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            user_id INTEGER,
            FOREIGN KEY (user_id) REFERENCES internal.user(user_id)
        );
        """)
        self.con.execute("""
        CREATE TABLE IF NOT EXISTS internal.table_metadata (
            table_name TEXT PRIMARY KEY,
            user_id_column TEXT,
            purpose_column TEXT,
            access_column TEXT
        );
        """)

        self.con.execute("INSERT INTO internal.user (user_id, username, admin, password) VALUES (?, ?, ?, ?)", None, (0, "William", False, "Password"))
        self.con.execute("INSERT INTO internal.user (user_id, username, admin, password) VALUES (?, ?, ?, ?)", None, (-1, "No Set Analyst", False, "SECRETPassword"))

    def analyst_exists(self, analyst_id):
        # Execute a query to find if the analyst_name exists
        result = self.con.execute("SELECT EXISTS(SELECT 1 FROM internal.user WHERE user_id = ? LIMIT 1)", None, (analyst_id,)).fetchone()[0]
        return result != None
        
    def log_query(self, query, analyst_id, query_id, query_plan):
        query = query.replace("'", "''")
        
        self.con.execute(f"INSERT INTO internal.query_log (query_id, query_text, query_plan, user_id) VALUES ({query_id}, '{query}', '{query_plan}', '{analyst_id}')")
        print(self.con.execute("SELECT * FROM internal.query_log"), None)

    def get_table_name_from_query_plan(self, query_plan):
        table_names = []
        if query_plan['table'] != '':
            table_names.append(query_plan['table'])
        for child in query_plan['children']:
            child_table_names = self.get_table_name_from_query_plan(child)
            table_names.extend(child_table_names)
        assert len(table_names) < 2, 'Multiple tables found in query plan'
        return table_names[0] if table_names else None

    def get_user_column_for_table(self, table_name):
        user_id_column = self.con.execute(f"SELECT user_id_column FROM internal.table_metadata WHERE table_name = '{table_name}'").fetchone()
        return user_id_column[0] if user_id_column else None

    def get_purposes_column_for_table(self, table_name):
        purposes_column = self.con.execute(f"SELECT purpose_column FROM internal.table_metadata WHERE table_name = '{table_name}'").fetchone()
        return purposes_column[0] if purposes_column else None

    def get_access_column_for_table(self, table_name):
        access_column = self.con.execute(f"SELECT access_column FROM internal.table_metadata WHERE table_name = '{table_name}'").fetchone()
        return access_column[0] if access_column else None

    def get_purpose_array(self, table_name, user_id):
        purpose_column_name = self.get_purposes_column_for_table(table_name)
        user_column_name = self.get_user_column_for_table(table_name)
        
        if purpose_column_name and user_column_name:
            query = f"SELECT {purpose_column_name} FROM {table_name} WHERE {user_column_name} = ?"
            result = self.con.execute(query, None, (user_id,)).fetchall()
            
            purpose_set = set()
            
            for row in result:
                if row[0]:
                    purposes = row[0].split(',')
                    purpose_set.update(purpose.strip() for purpose in purposes)  # Remove any surrounding whitespace

            return list(purpose_set)
        else:
            return []
