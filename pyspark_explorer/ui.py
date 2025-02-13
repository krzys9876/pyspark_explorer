import asyncio

from textual import on, work
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Vertical
from textual.widgets import DataTable, Header, Footer, Static, Tree, TabbedContent, Select, TabPane
from textual.widgets._tree import TreeNode

from pyspark_explorer.data_table import DataFrameTable, extract_embedded_table
from pyspark_explorer.explorer import Explorer
from pyspark_explorer.ui_busy_screen import BusyScreen
from pyspark_explorer.ui_filters_screen import FiltersScreen
from pyspark_explorer.ui_options_screen import OptionsScreen


class PysparkExplorerUI(App):

    FILE_TYPES = ["PARQUET", "JSON", "CSV"]

    CSS_PATH = "ui.tcss"

    BINDINGS = [
        #Binding(key="question_mark", action="help", description="Show help screen", key_display="?"),
        Binding(key="d", action="refresh_current_directory", description="Read directory"),
        Binding(key="D", action="refresh_current_directory_with_filter", description="with filter"),
        Binding(key="f", action="read_file", description="Read file"),
        Binding(key="F", action="read_file_with_filter", description="with filter"),
        Binding(key="r", action="reload_table", description="Reset data view"),
        Binding(key="b", action="drill_up", description="Previous data view"),
        Binding(key="u", action="refresh_table", description="Refresh view", show=False),
        Binding(key="o", action="change_options", description="Options"),
        Binding(key="s", action="save_options", description="Save config"),
        Binding(key="1", action="view_narrow", description="1:8"),
        Binding(key="2", action="view_normal", description="1:3"),
        Binding(key="3", action="view_wide", description="1:1"),
        Binding(key="^q", action="Force quit", description="Quit the app"),
    ]


    def __init__(self, explorer: Explorer, **kwargs):
        super(PysparkExplorerUI, self).__init__(**kwargs)
        self.orig_tab: DataFrameTable = DataFrameTable([],[])
        self.tab = self.orig_tab
        self.tab_stack: [dict] = []
        self.explorer = explorer
        self.file_type = self.FILE_TYPES[0]
        self.current_file = None


    def compose(self) -> ComposeResult:
        with Static(content="", id="main_container"):
            yield Header()
            with Vertical(id="top_container"):
                yield Static("", id="top_status")
            with Vertical(id="left_container"):
                with TabbedContent(id="tab_container"):
                    with TabPane(id="files_pane", title="Files"):
                        # Files tab
                        with Vertical(id="files_container"):
                            yield Select.from_values(self.FILE_TYPES, allow_blank=False, value=self.file_type, id="file_type_select")
                            yield Tree("",id="file_tree")
                    with TabPane(id="struct_pane", title="Structure"):
                        # Structure tab
                        yield Tree("",id="struct_tree")

            with Vertical(id="main_table_container"):
                yield DataTable(id="main_table")
                yield Static("", id="main_table_status")

            yield Static("", id="bottom_left_status")
            yield Static("", id="bottom_mid_status")
            yield Footer(show_command_palette=True)

    def __main_container__(self) -> Static:
        return self.get_widget_by_id(id="main_container", expect_type=Static)

    def __tab_container__(self) -> TabbedContent:
        return self.get_widget_by_id(id="tab_container", expect_type=TabbedContent)

    def __main_table__(self) -> DataTable:
        return self.get_widget_by_id(id="main_table", expect_type=DataTable)

    def __main_table_status__(self) -> Static:
        return self.get_widget_by_id(id="main_table_status", expect_type=Static)

    def __top_status__(self) -> Static:
        return self.get_widget_by_id(id="top_status", expect_type=Static)

    def __struct_tree__(self) -> Tree:
        return self.get_widget_by_id(id="struct_tree", expect_type=Tree)

    def __files_tree__(self) -> Tree:
        return self.get_widget_by_id(id="file_tree", expect_type=Tree)

    def __bottom_left_status__(self) -> Static:
        return self.get_widget_by_id(id="bottom_left_status", expect_type=Static)

    def __bottom_mid_status__(self) -> Static:
        return self.get_widget_by_id(id="bottom_mid_status", expect_type=Static)


    def on_mount(self) -> None:
        self.title = "Spark File Explorer"
        self.sub_title = "explore files, structure and data"

        self.__read_base_path__()
        self.__refresh_top_status__()
        self.action_reload_table(refresh_view = False)
        self.__tab_container__().active = "files_pane"


    def __read_base_path__(self) -> None:
        file_tree = self.__files_tree__()
        file_tree.clear()
        base_info = self.explorer.file_info(self.explorer.get_base_path())
        root_label = self.__file_label__(base_info)
        file_tree.root.set_label(root_label)
        file_tree.root.data = base_info


    @on(Select.Changed)
    def select_changed(self, event: Select.Changed) -> None:
        if event.select.id=="file_type_select":
            self.file_type = str(event.value)
            self.notify(f"{self.file_type} selected")


    def load_data(self, refresh_view: bool = True) -> None:
        data_table = self.__main_table__()
        data_table.loading = True
        data_table.clear(columns=True)
        data_table.add_columns(*self.tab.column_names)
        data_table.add_rows(self.tab.row_values)
        data_table.loading = False
        if refresh_view:
            self.action_refresh_table()


    @staticmethod
    def __add_subfields_to_tree(field_info: {}, node: TreeNode):
        if field_info["kind"] == "simple":
            node.add_leaf(f"{field_info['name']} ({field_info['type']})", data=field_info)
        else:
            added = node.add(f"{field_info['name']} ({field_info['type']})", data=field_info)
            for subfield in field_info["subfields"]:
                PysparkExplorerUI.__add_subfields_to_tree(subfield, added)
            added.expand()


    def load_structure(self) -> None:
        tree: Tree = self.__struct_tree__()
        tree.clear()
        tree.show_root = False
        tree.auto_expand = True

        for f in self.tab.schema_tree:
            self.__add_subfields_to_tree(f, tree.root)

    @work
    async def action_reload_table(self, refresh_view: bool = True) -> None:
        self.tab = self.orig_tab
        # reset stack
        self.tab_stack = [{"tab": self.tab, "row": 0, "col": 0}]
        self.load_data(refresh_view)
        self.load_structure()


    def action_refresh_table(self) -> None:
        # experimental - refresh by getting table out of focus and focus again, no other method worked (refresh etc.)
        self.set_focus(self.__tab_container__())
        self.__tab_container__().active = "struct_pane"
        self.set_focus(self.__main_table__())


    @staticmethod
    def __file_label__(info: {}) -> str:
        if info["is_dir"]:
            # label = f"\uea83 {info["name"]}"
            label = f"{info['name']}"
        else:
            # label = f"\uf15c {info["name"]} {info["hr_size"]}"
            label = f"{info['name']} {info['hr_size']}"
        return label


    def action_refresh_current_directory(self) -> None:
        self.__refresh_current_directory__("*")


    @work
    async def action_refresh_current_directory_with_filter(self) -> None:
        res = await self.push_screen_wait(FiltersScreen("Enter filter for files and dirs", self.explorer.get_file_filters()))
        if res is None:
            pass
        elif len(res) == 0:
            self.notify(f"No filename filter selected")
        else:
            self.notify(f"Filter selected: {res}")
            self.explorer.add_as_first_file_filter(res)
            self.__refresh_current_directory__(res)


    def __refresh_current_directory__(self, file_filter: str) -> None:
        current_file = self.__files_tree__().cursor_node
        if current_file is None:
            self.notify(f"No file/directory selected")
            return

        if not current_file.data["is_dir"]:
            self.notify(f"File (not directory) is selected {current_file.data}")
            return

        self.set_focus(self.__files_tree__())
        self.notify(f"Refreshing {current_file.data['name']}") # {current_file.data}")
        path = current_file.data["full_path"]
        dir_contents = self.explorer.read_directory(path,file_filter)
        current_file.remove_children()
        for f in dir_contents:
            label = self.__file_label__(f)
            if f["is_dir"]:
                current_file.add(label=label, data=f)
            else:
                current_file.add_leaf(label=label, data=f)

        current_file.expand()


    def action_read_file(self) -> None:
        self.__action_read_file__("1=1")


    @work
    async def action_read_file_with_filter(self) -> None:
        res = await self.push_screen_wait(FiltersScreen("Enter spark filter", self.explorer.get_spark_filters()))
        if res is None:
            pass
        elif len(res) == 0:
            self.notify(f"No filter selected")
        else:
            self.notify(f"Filtering file with: {res}")
            self.explorer.add_as_first_spark_filter(res)
            self.__action_read_file__(res)


    def __action_read_file__(self, spark_filter: str) -> None:
        #self.query_one(LoadingIndicator).display = True
        current_file = self.__files_tree__().cursor_node
        if current_file is None:
            self.notify(f"No file/directory selected")
            return

        if not current_file.data["is_dir"] and current_file.data["size"]==0:
            self.notify(f"Cannot read file of zero length")
            return

        if not current_file.data["is_dir"] and current_file.data["type"] in self.FILE_TYPES:
            file_type = current_file.data["type"]
        else:
            file_type = self.file_type

        self.__read_file__(current_file.data["full_path"], file_type, spark_filter)


    @work
    async def __read_file__(self, path: str, file_type: str, spark_filter: str) -> None:
        self.notify(f"Reading file as {file_type}\n{path}")

        await self.push_screen(BusyScreen())
        await asyncio.sleep(1)
        self.refresh()
        tab, result_descr = self.explorer.read_file(file_type, path, spark_filter)
        await self.pop_screen()

        if tab is None:
            self.notify(f"Error occurred reading file: {path}\nSee log file for details (user_home/.pyspark-explorer)\n{result_descr}")
        else:
            self.orig_tab = tab
            self.current_file = path
            self.__refresh_top_status__()
            self.action_reload_table()

    @work
    async def action_change_options(self) -> None:
        res: dict = await self.push_screen_wait(OptionsScreen(self.explorer.params))
        if res is None:
            pass
        elif res == self.explorer.params:
            self.notify(f"No options have been changed")
        else:
            self.notify(f"Options changed from {self.explorer.params} to: {res}")
            base_path_changed = self.explorer.get_base_path()!=res["base_path"]
            self.explorer.params = res
            if base_path_changed:
                self.__read_base_path__()
            self.__refresh_top_status__()


    def action_save_options(self) -> None:
        self.notify("Saving config to file (user_home/.pyspark-explorer)")
        self.explorer.save_params()


    def __refresh_top_status__(self) -> None:
        status = self.__top_status__()
        base_path = self.explorer.get_base_path()
        path = self.current_file if self.current_file is not None else ""
        path_fragment = path[len(base_path):] if len(path) > len(base_path) and path[:len(base_path)] == base_path else path
        status_txt = (f"Base path: {self.explorer.get_base_path()} | Loaded file: {path_fragment}\n" +
                      f"Read rows: {self.explorer.get_take_rows()} | Show files: {self.explorer.get_file_limit()} | "+
                      f"Sort files {'desc.' if self.explorer.get_sort_files_desc() else 'asc.'}")
        status.update(status_txt)


    def __selected_cell_info__(self) -> (int, int, {}):
        main_tab = self.__main_table__()
        x = main_tab.cursor_column
        y = main_tab.cursor_row
        column, cell = self.tab.select(x,y)
        return x, y, column, cell


    @on(DataTable.CellHighlighted, "#main_table")
    def cell_highlighted(self, _: DataTable.CellHighlighted):
        x, y, column, cell = self.__selected_cell_info__()
        pos_txt = f"{x+1}/{y+1}"
        cell_dv = cell["display_value"]
        dv_status = self.__bottom_mid_status__()
        dv_status.update(cell_dv)
        status_text_flat = f"{column['name']} | {column['type']}/{column['field_type'].typeName()} | {column['kind']}"
        if column["type"]=="ArrayType":
            status_text_flat = f"{status_text_flat} | {len(cell['value'])} inner row(s)"

        main_table_status = self.__main_table_status__()
        main_table_status.update(f"{pos_txt} | {status_text_flat}")


    @on(DataTable.CellSelected, "#main_table")
    def cell_selected(self, _: DataTable.CellSelected):
        x, y, _, _ = self.__selected_cell_info__()
        embedded_tab = extract_embedded_table(self.tab, x, y, expand_structs = True)
        if embedded_tab is None:
            self.notify("no further details available")
        else:
            self.notify(f"drilling down into details: {len(embedded_tab.row_values)} row(s)")
            self.tab = embedded_tab
            self.tab_stack.append({"tab": embedded_tab, "row": y, "col": x})
            self.load_data()


    def action_drill_up(self) -> None:
        if len(self.tab_stack)>=2:
            # retrieve return cursor position
            row: int = self.tab_stack[-1]["row"]
            col: int = self.tab_stack[-1]["col"]
            # remove last tab info (tab is stored together with return cursor position)
            self.tab_stack.pop()
            # retrieve previous tab
            self.tab = self.tab_stack[-1]["tab"]
            self.notify(f"drilling up to previous view")
            self.load_data()
            # move cursor to return position
            main_tab = self.__main_table__()
            main_tab.move_cursor(row = row, column = col)

        else:
            self.notify(f"this is the top view of the current file")


    @on(Tree.NodeHighlighted, "#file_tree")
    def file_selected(self, event: Tree.NodeHighlighted):
        data = event.node.data
        type_status = self.__bottom_left_status__()
        if data is None:
            type_status.update("")
            return

        if data["is_dir"]:
            status_text = f"{data['name']} (dir)"
        else:
            status_text = f"{data['name']}\n{data['type']} {data['hr_size']} ({data['size']})"
        type_status.update(status_text)


    def action_view_narrow(self) -> None:
        self.add_class("narrow")
        self.remove_class("wide")


    def action_view_normal(self) -> None:
        self.remove_class("narrow")
        self.remove_class("wide")


    def action_view_wide(self) -> None:
        self.remove_class("narrow")
        self.add_class("wide")
