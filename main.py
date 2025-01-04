from query_parser_package.query_parser import QueryParser
from query_parser_package.query_tokenizer import QueryTokenizer
from settings import AVAILABLE_QUERIES
from utils.errors import ParseError, TableError


def list_queries():
    for query in AVAILABLE_QUERIES:
        print(query)


print("Available commands:\nq - query the database\nl - list the possible queries\ne - exit")
print("-----------------------")

while True:
    command = input("> ")

    if command == "q":
        query = input("Query: ")
        tokenizer = QueryTokenizer(query)
        tokens = tokenizer.tokenize()
        parser = QueryParser(tokens)

        try:
            parsed_query = parser.parse()
            parsed_query.execute_statement()
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
    elif command == "e":
        break
    else:
        print("Unknown command")
