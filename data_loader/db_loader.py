from reader.JSONFileReader import JSONFileReader
from modules.db.DatabaseSessionBuilder import DatabaseSessionBuilder
from utils.utils import log,error
from rich.console import Console
import  datetime
console = Console()

class DatabaseManager:
    def __init__(self, config_path):
            json_reader = JSONFileReader(config_path)
            self.config = json_reader.read()

    def get_connection(self, db_name):
        db_config = next((db for db in self.config if db["database"] == db_name), None)
        if db_config:
            if db_config['type'] == 'postgresql':
                conn_str = f"postgresql://{db_config['username']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
            elif db_config['type'] == 'mysql':
                conn_str = f"mysql+pymysql://{db_config['username']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
            else:
                #raise ValueError(f"Unsupported database type: {db_config['type']}")
                error("Unsupported database type %s. Not present!" % (db_name))
        else:
         error("No database found with the name:", db_name)
        db_session = DatabaseSessionBuilder(conn_str)

        if DatabaseSessionBuilder:
            print("")
            console.print(f"Database connected successfully database name : [bold white]{db_name}[/bold white]")
        return db_session