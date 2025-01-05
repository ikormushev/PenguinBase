import os
from query_parser_package.query_parser import QueryParser
from query_parser_package.query_tokenizer import QueryTokenizer
from settings import PBDB_FILES_PATH, PROJECT_PATH, AVAILABLE_QUERIES
from utils.errors import ParseError, TableError
from utils.string_utils import custom_strip, custom_split
import tkinter as tk
from tkinter import ttk, messagebox


class CustomDBMSAPI:
    def execute_query(self, query_str: str):
        tokenizer = QueryTokenizer(query_str)
        tokens = tokenizer.tokenize()
        parser = QueryParser(tokens)

        parsed_query = parser.parse()

        return parsed_query.execute_statement()

    def list_tables(self, db_folder: str):
        tables = []
        for name in os.listdir(db_folder):
            dir_path = os.path.join(db_folder, name)
            if os.path.isdir(dir_path):
                data_file = os.path.join(dir_path, f"{name}.data")
                meta_file = os.path.join(dir_path, f"{name}.meta")
                if os.path.exists(data_file) and os.path.exists(meta_file):
                    tables.append(name)
        return tables


class DatabaseGUI:
    def __init__(self, root, tables_directory: str):
        self.directory = tables_directory
        self.root = root
        self.root.title("PenguinBase")
        self.root.geometry("1200x800")
        self.root.minsize("1100", "800")

        self.db_api = CustomDBMSAPI()
        style = ttk.Style(root)
        style.theme_use("clam")
        style.configure("Treeview", rowheight=25, font=("Helvetica", 10))
        style.configure("Treeview.Heading", font=("Helvetica", 11, "bold"))

        self.status_label = tk.Label(self.root, text="Ready", bd=1, relief=tk.SUNKEN, anchor=tk.W)
        self.status_label.pack(side=tk.BOTTOM, fill=tk.X)

        self._create_menu()

        main_frame = tk.Frame(self.root, padx=10, pady=10)
        main_frame.pack(fill=tk.BOTH, expand=True)

        left_frame = tk.Frame(main_frame)
        left_frame.pack(side=tk.LEFT, fill=tk.Y)

        home_btn = tk.Button(
            left_frame,
            text="🏠 Home Page",
            font=("Helvetica", 12, "bold"),
            bg="#51ADED",
            command=self.show_home_page,
            padx=10
        )
        home_btn.pack(side=tk.TOP, fill=tk.X, pady=5)
        home_btn.bind("<Enter>", lambda e: home_btn.config(cursor="hand2"))
        home_btn.bind("<Leave>", lambda e: home_btn.config(cursor="arrow"))

        header_label = tk.Label(
            left_frame,
            text="Tables",
            font=("Helvetica", 14, "bold"),
            bg="mediumpurple1",
            fg="white",
        )
        header_label.pack(side=tk.TOP, fill=tk.X)
        table_list_scroll_y = tk.Scrollbar(left_frame, orient=tk.VERTICAL)
        table_list_scroll_y.pack(side=tk.RIGHT, fill=tk.Y)
        table_list_scroll_x = tk.Scrollbar(left_frame, orient=tk.HORIZONTAL)
        table_list_scroll_x.pack(side=tk.BOTTOM, fill=tk.X)
        self.table_list = tk.Listbox(left_frame,
                                     width=20, yscrollcommand=table_list_scroll_y.set,
                                     xscrollcommand=table_list_scroll_x.set)
        self.table_list.pack(side=tk.LEFT, fill=tk.Y, expand=True)
        table_list_scroll_y.config(command=self.table_list.yview)
        table_list_scroll_x.config(command=self.table_list.xview)
        self.table_list.bind("<<ListboxSelect>>", self.on_table_select)

        self.right_frame = tk.Frame(main_frame)
        self.right_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        self.home_frame = tk.Frame(self.right_frame)
        self._create_home_page()

        self.dbms_frame = tk.Frame(self.right_frame)
        self._create_dbms_page()

        control_frame = tk.Frame(self.root)
        control_frame.pack(side=tk.BOTTOM, fill=tk.X)

        self.query_entry = tk.Entry(control_frame, width=60)
        self.query_entry.pack(side=tk.LEFT, padx=5, pady=5)
        exec_btn = tk.Button(
            control_frame,
            text="Execute",
            command=self.on_execute_query,
            font=("Helvetica", 12, "bold"),
            bg="mediumpurple1",
            fg="white",
        )
        exec_btn.pack(side=tk.LEFT, padx=5, pady=5)
        exec_btn.bind("<Enter>", lambda x: exec_btn.config(bg="lightblue", fg="black", cursor="hand2"))
        exec_btn.bind("<Leave>", lambda x: exec_btn.config(bg="mediumpurple1", fg="white", cursor="arrow"))

        self._populate_table_list()
        self.set_status("PenguinBase started!")
        self.show_frame(self.home_frame)
        self.current_table = None
        self.current_generator = None
        self.current_page = 0
        self.cached_rows = []

    def _create_menu(self):
        menubar = tk.Menu(self.root)

        file_menu = tk.Menu(menubar, tearoff=0)
        file_menu.add_command(label="Exit", command=self.root.quit)
        menubar.add_cascade(label="File", menu=file_menu)

        tables_menu = tk.Menu(menubar, tearoff=0)
        tables_menu.add_command(label="Refresh Tables", command=self._populate_table_list)
        menubar.add_cascade(label="Tables", menu=tables_menu)

        self.root.config(menu=menubar)

    def _create_table_info(self, parent_frame):
        table_info_frame = tk.Frame(parent_frame, height=100)

        table_info_scroll_y = tk.Scrollbar(table_info_frame, orient=tk.VERTICAL)
        table_info_scroll_y.pack(side=tk.RIGHT, fill=tk.Y)

        table_info_tree = ttk.Treeview(
            table_info_frame,
            columns=["Statistic", "Value"],
            show="headings",
            yscrollcommand=table_info_scroll_y.set,
            height=5
        )
        table_info_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        table_info_scroll_y.config(command=table_info_tree.yview)

        table_info_tree.heading("Statistic", text="Statistic")
        table_info_tree.column("Statistic", width=200, minwidth=150, anchor=tk.W)
        table_info_tree.heading("Value", text="Value")
        table_info_tree.column("Value", width=400, minwidth=200, anchor=tk.W)

        return table_info_frame, table_info_tree

    def _create_dbms_page(self):
        self.query_heading = tk.Label(self.dbms_frame, text="", font=("Helvetica", 16, "bold"))
        self.query_heading.pack(side=tk.TOP, pady=10)

        self.table_info_frame, self.table_info_tree = self._create_table_info(self.dbms_frame)
        self.table_info_frame.pack(side=tk.TOP, fill=tk.X, padx=5, pady=5)

        tree_scroll_y = tk.Scrollbar(self.dbms_frame, orient=tk.VERTICAL)
        tree_scroll_y.pack(side=tk.RIGHT, fill=tk.Y)
        tree_scroll_x = tk.Scrollbar(self.dbms_frame, orient=tk.HORIZONTAL)
        tree_scroll_x.pack(side=tk.BOTTOM, fill=tk.X)

        self.tree = ttk.Treeview(
            self.dbms_frame,
            columns=["#0", "#1", "#2"],
            show="headings",
            yscrollcommand=tree_scroll_y.set,
            xscrollcommand=tree_scroll_x.set
        )
        self.tree.pack(side=tk.TOP, fill=tk.BOTH)
        tree_scroll_y.config(command=self.tree.yview)
        tree_scroll_x.config(command=self.tree.xview)

        control_frame = tk.Frame(self.dbms_frame)
        control_frame.pack(side=tk.BOTTOM, fill=tk.X)

        self.next_btn = tk.Button(
            control_frame,
            text="Next Page",
            font=("Helvetica", 12, "bold"),
            bg="#51ADED",
            padx=2,
            command=self.on_next_page)
        self.next_btn.bind("<Enter>", lambda e: self.next_btn.config(cursor="hand2"))
        self.next_btn.bind("<Leave>", lambda e: self.next_btn.config(cursor="arrow"))
        self.next_btn.pack(side=tk.LEFT, padx=5, pady=5)

    def _create_home_page(self):
        top_frame = tk.Frame(self.home_frame)
        top_frame.pack(side=tk.TOP, fill=tk.X, pady=10)

        welcome_label = tk.Label(
            top_frame,
            text="Welcome to PenguinBase!",
            font=("Helvetica", 18, "bold"),
            bg="#51ADED",
            fg="white",
            padx=20,
            pady=10
        )
        welcome_label.pack(side=tk.TOP, pady=10)

        instructions = tk.Label(
            top_frame,
            text="Select a table from the list on the left to view its contents.\n"
                 "You can also execute manual queries using the query entry below.",
            font=("Helvetica", 12)
        )
        instructions.pack(side=tk.TOP, pady=5)

        middle_frame = tk.Frame(self.home_frame)
        middle_frame.pack(side=tk.TOP, fill=tk.BOTH, expand=True, pady=10)

        logo_frame = tk.Frame(middle_frame)
        logo_frame.pack(side=tk.LEFT, fill=tk.Y, padx=10)

        logo_path = os.path.join(PROJECT_PATH, 'resources/database-logo.png')
        if os.path.exists(logo_path):
            self.home_image = tk.PhotoImage(file=logo_path)
            image_label = tk.Label(logo_frame, image=self.home_image)
            image_label.pack(side=tk.TOP, pady=10)

        queries_frame = tk.Frame(middle_frame)
        queries_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=10)

        queries_label = tk.Label(
            queries_frame,
            text="Available Queries:",
            font=("Helvetica", 14, "bold"),
            fg="#d9534f",
            padx=10,
        )
        queries_label.pack(side=tk.TOP, anchor="w", pady=(15, 5))

        separator = tk.Frame(queries_frame, bg="#d9534f", height=2, width=400)
        separator.pack(side=tk.TOP, pady=(0, 10), fill=tk.X)

        queries_text = tk.Text(
            queries_frame,
            height=28,
            font=("Courier", 11),
            bg="#ffffff",
            fg="#333333",
            wrap=tk.WORD,
            relief=tk.GROOVE,
            bd=2,
        )
        queries_text.pack(side=tk.TOP, fill=tk.BOTH)

        for query in AVAILABLE_QUERIES:
            queries_text.insert(tk.END, f"{query}\n\n")

        queries_text.config(state=tk.DISABLED)

    def _populate_table_list(self):
        self.table_list.delete(0, tk.END)

        tables = self.db_api.list_tables(self.directory)
        for t in tables:
            self.table_list.insert(tk.END, t)

        self.set_status("Tables refreshed.")
        self.show_frame(self.home_frame)
        self.current_table = None

    def show_frame(self, frame):
        for widget in self.right_frame.winfo_children():
            widget.pack_forget()
        frame.pack(fill=tk.BOTH, expand=True)

    def show_home_page(self):
        self.set_status("Home Page loaded.")
        self.show_frame(self.home_frame)
        self.current_table = None

    def on_table_select(self, event):
        selection = self.table_list.curselection()
        if not selection:
            return
        index = selection[0]
        table_name = self.table_list.get(index)

        self.set_status(f"Loading table: {table_name}")
        self._execute_query(f"SELECT * FROM {table_name};", is_table_select=True)

    def populate_table_info(self, table_info):
        self.table_info_tree.delete(*self.table_info_tree.get_children())
        split_general_info = custom_split(table_info["general"], "\n")
        for info in split_general_info:
            if info:
                stat, value = custom_split(info, ":")
                self.table_info_tree.insert("", tk.END, values=(stat, value))

        split_column_info = custom_split(table_info["columns"], "\n")
        for info in split_column_info:
            if info:
                stat, value = custom_split(info, "|")
                self.table_info_tree.insert("", tk.END, values=(f"Column: {stat}", value))

        split_index_info = custom_split(table_info["indexes"], "\n")

        for info in split_index_info:
            if info:
                stat, value = custom_split(info, ":")
                self.table_info_tree.insert("", tk.END, values=(f"{stat} size", value))

    def display_metadata(self, table_info):
        self.tree.pack_forget()
        self.next_btn.pack_forget()
        self.table_info_frame.pack(side=tk.TOP, fill=tk.X, padx=5, pady=5)
        self.populate_table_info(table_info)

    def display_rows(self, columns):
        self.tree.pack(side=tk.TOP, fill=tk.BOTH, expand=True)
        self.next_btn.pack(side=tk.LEFT, padx=5, pady=5)
        self.tree.delete(*self.tree.get_children())
        cols = ["Row Number"] + [col_name for col_name, col in columns.items()]

        self.tree["columns"] = cols
        self.tree["show"] = "headings"

        total_width = self.tree.winfo_width()
        default_width = max(200, total_width // len(cols))

        for col_name in cols:
            self.tree.heading(col_name, text=col_name)
            self.tree.column(col_name, width=default_width, minwidth=150, stretch=True, anchor=tk.W)

        self.current_page = 0
        self.cached_rows = []
        self._show_next_page(self.current_page)

    def display_table_rows(self, columns, table_info, is_table_select):
        self.table_info_frame.pack_forget()
        if is_table_select:
            self.display_metadata(table_info)
        self.display_rows(columns)

    def on_next_page(self):
        if not self.current_table:
            messagebox.showinfo("Info", "No table selected.")
            return

        self.current_page += 1
        self._show_next_page(self.current_page)

    def _show_next_page(self, page_num):
        self.tree.delete(*self.tree.get_children())
        rows_fetched = 0
        row_number = page_num * 100 + 1

        try:
            while rows_fetched < 100:
                row = next(self.current_generator)
                if rows_fetched == 0:
                    self.cached_rows.clear()
                col_vals = [row_number]
                row_number += 1
                for col_name in self.tree["columns"][1:]:
                    val = row[col_name]
                    col_vals.append(str(val))
                self.cached_rows.append(col_vals)

                # self.tree.insert("", tk.END, values=col_vals)
                rows_fetched += 1
        except StopIteration:
            if (rows_fetched == 0 and self.current_table.metadata.rows_count != 0
                    or (self.current_table.metadata.rows_count == 0 and self.current_page > 0)):
                messagebox.showinfo("Info", "No rows for the table!")
                self.current_page -= 1

        if self.cached_rows:
            self.tree.delete(*self.tree.get_children())
            for row_data in self.cached_rows:
                self.tree.insert("", tk.END, values=row_data)

        self.set_status(f"Showing page {self.current_page + 1}, fetched {rows_fetched} rows.")

    def _execute_query(self, query: str, is_table_select: bool = False):
        try:
            result = self.db_api.execute_query(query)
            message = result["message"]
            if message:
                self.set_status(message)

            table = result["table"]
            rows = result["rows"]
            columns = result["columns"]
            tableinfo = result["tableinfo"]
            table_action = result["table_action"]
            if table and rows and columns:
                self.current_table = table
                self.current_generator = rows
                columns = columns
                table_info = self.current_table.tableinfo()

                self.show_frame(self.dbms_frame)
                self.display_table_rows(columns, table_info, is_table_select)
                self.query_heading.config(text=f"{table.table_name}")
            elif tableinfo and table:
                self.show_frame(self.dbms_frame)
                self.query_heading.config(text=f"Table Information - {table.table_name}")
                self.display_metadata(tableinfo)
            else:
                self.show_frame(self.home_frame)
                self.current_table = None
                messagebox.showinfo("Query", f"Query executed successfully!")

                if table_action:
                    self._populate_table_list()
        except ParseError as e:
            messagebox.showerror("Parse Error", f"{e}")
            print(f"Invalid query: {e}")
        except TableError as e:
            messagebox.showerror("Table Error", f"{e}")
            print(f"Error with table: {e}")
        except ValueError as e:
            messagebox.showerror("Value Error", f"{e}")
            print(f"Invalid parsing: {e}")
        except TypeError as e:
            messagebox.showerror("Type Error", f"{e}")
            print(f"Invalid type operation: {e}")
        except Exception as e:
            messagebox.showerror("Error", f"{e}")
            print(f"General Error: {e}")

    def on_execute_query(self):
        query_str = self.query_entry.get()
        if not custom_strip(query_str):
            messagebox.showerror("Parse Error", "Query cannot be empty!")
            return

        self.set_status("Executing query...")
        self._execute_query(query_str)
        # self.query_entry.delete(0, tk.END) - empty the query field

    def set_status(self, msg: str):
        self.status_label.config(text=msg)


if __name__ == "__main__":
    root = tk.Tk()
    app = DatabaseGUI(root, PBDB_FILES_PATH)
    root.mainloop()
