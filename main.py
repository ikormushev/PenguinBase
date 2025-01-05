from data_structures.hash_table import HashTable
from query_parser_package.query_parser import QueryParser
from query_parser_package.query_tokenizer import QueryTokenizer
from settings import AVAILABLE_QUERIES
from utils.errors import ParseError, TableError
from utils.string_utils import custom_split


def list_queries():
    for query in AVAILABLE_QUERIES:
        print(query)


def list_table_info(table_info: HashTable):
    split_general_info = custom_split(table_info["general"], "\n")
    print("General info:")
    for info in split_general_info:
        if info:
            stat, value = custom_split(info, ":")
            print(f"\t{stat}: {value}")

    split_column_info = custom_split(table_info["columns"], "\n")
    print("Column info:")
    for info in split_column_info:
        if info:
            stat, value = custom_split(info, "|")
            print(f"\t{stat}: {value}")

    split_index_info = custom_split(table_info["indexes"], "\n")
    print("Index info:")
    for info in split_index_info:
        if info:
            stat, value = custom_split(info, ":")
            print(f"\t{stat}: {value}")


def list_commands():
    print("-----------------------")
    print("Available commands:\n"
          "q - query the database\n"
          "l - list the possible queries\n"
          "c - list the available commands\n"
          "e - exit")
    print("-----------------------")

list_commands()

while True:
    command = input("> ")

    if command == "q":
        query = input("Query: ")
        tokenizer = QueryTokenizer(query)
        tokens = tokenizer.tokenize()
        parser = QueryParser(tokens)

        try:
            parsed_query = parser.parse()
            result = parsed_query.execute_statement()
            rows = result["rows"]
            columns = result["columns"]

            if rows is not None and columns is not None:
                curr_row_count = 0
                rows = result["rows"]
                cols = ["Row Number"] + [col_name for col_name, col in columns.items()]
                print("  |  ".join(cols))

                for row in rows:
                    curr_row_count += 1
                    print(f"  {curr_row_count}  |  ", end="")
                    spread_row_value = [str(value) for _, value in row.items()]
                    print("  |  ".join(spread_row_value))
                continue

            if result["tableinfo"] is not None:
                list_table_info(result["tableinfo"])

        except ParseError as e:
            print(f"Invalid query: {e}")
        except TableError as e:
            print(f"Error with table: {e}")
        except ValueError as e:
            print(f"Invalid parsing: {e}")
        except TypeError as e:
            print(f"Invalid type operation: {e}")
        except Exception as e:
            print(f"General Error: {e}")
    elif command == "l":
        list_queries()
    elif command == "c":
        list_commands()
    elif command == "e":
        break
    else:
        print("Unknown command")
