import os

PROJECT_PATH = os.path.dirname(os.path.abspath(__file__))
PBDB_FILES_PATH = os.path.join(PROJECT_PATH, 'pbdb_files')
AVAILABLE_QUERIES = [
    "CREATE TABLE <table_name> (col1:type CONSTRANINT1:value ..., col2:type CONSTRANINT1:value ..., ...);",
    "DROP TABLE <table_name>;",
    "TABLEINFO <table_name>;",
    "INSERT INTO <table_name> (col1, col2, ...) VALUES (val1, val2, ...), ...;",
    "GET ROW row_number_1, row_number_2, ... FROM <table_name>;",
    "DELETE FROM <table_name> ROW row_number_1, row_number_2, ...;",
    "DELETE FROM <table_name> WHERE <expression>;",
    "SELECT [DISTINCT] [col1, col2, ...] FROM <table_name> [WHERE <expr>] [ORDER BY ...];",
    "CREATE INDEX <index_name> ON <table_name> (column_name);",
    "DROP INDEX <index_name> ON <table_name>;",
    "DEFRAGMENT <table_name>;"
]
