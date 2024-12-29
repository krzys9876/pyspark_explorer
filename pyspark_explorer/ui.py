import asyncio

from textual import on, work
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Vertical
from textual.screen import Screen
from textual.widgets import DataTable, Header, Footer, Static, Input, Tree, TabbedContent, RadioSet, RadioButton, \
    LoadingIndicator
from textual.widgets._tree import TreeNode

from pyspark_explorer.data_table import DataFrameTable, extract_embedded_table
from pyspark_explorer.explorer import Explorer


class BusyScreen(Screen):
    CSS = """
    BusyScreen {
        align: center middle;
        background: rgba(0,0,0,0.5);
    }
    #dialog {
        background: $secondary;
        height: 20;
        width: 30;
        align: center middle;
    }
    #label {
        align: center middle;
    }    
    """

    def compose(self) -> ComposeResult:

        with Vertical(id = "dialog"):
            yield Static(content=" Waiting for spark session... ", id="label")



class DataApp(App):

    def __init__(self, explorer: Explorer, base_path: str, **kwargs):
        super(DataApp, self).__init__(**kwargs)
        self.orig_tab: DataFrameTable = DataFrameTable([],[])
        self.tab = self.orig_tab
        self.base_path = base_path
        self.explorer = explorer
        self.file_type = "JSON"


    CSS = """
        Screen {
            layout: grid;
            grid-size: 2 3;
            grid-columns: 1fr 5fr;
            grid-rows: 3 5fr 5;
        }
        #top_container {
            column-span: 2;
            background: $secondary;
            height: 100%;
        }
        #left_container {
            background: $boost;
            height: 100%;
        }
        #main_table_container {
            layout: grid;
            grid-size: 1 2;
            grid-rows: 1fr 1;
        }
        #main_table {
        }
        #main_table_status {
            width: 100%;
            background: $secondary;
        }
        #bottom_left_status {
            background: $secondary;
            height: 100%;
        }
        #bottom_mid_status {
            background: $boost;
            height: 100%;
        }
        #bottom_right_status {
            background: $secondary;
            height: 100%;
        }
        """

    BINDINGS = [
        Binding(key="^q", action="quit", description="Quit the app"),
        #Binding(key="question_mark", action="help", description="Show help screen", key_display="?"),
        Binding(key="r", action="reload_table", description="Reload current file"),
        Binding(key="u", action="refresh_table", description="Refresh table", show=False),
        Binding(key="d", action="refresh_current_directory", description="Refresh directory"),
        Binding(key="f", action="read_file", description="Read file sample"),
    ]


    def compose(self) -> ComposeResult:
        #yield LoadingIndicator()
        yield Header()
        with Vertical(id="top_container"):
            yield Static("", id="top_status")
        with Vertical(id="left_container"):
            with TabbedContent("Files", "Structure"):
                # Files tab
                with Vertical(id="files_container"):
                    with RadioSet(id="file_type"):
                        yield RadioButton("PARQUET")
                        yield RadioButton("JSON", value=True)
                        yield RadioButton("CSV")
                    yield Tree("",id="file_tree")

                # Structure tab
                yield Tree("",id="struct_tree")

        with Vertical(id="main_table_container"):
            yield DataTable(id="main_table")
            yield Static("", id="main_table_status")

        yield Static("", id="bottom_left_status")
        yield Static("", id="bottom_mid_status")
        yield Footer(show_command_palette=True)


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

    def __file_type_radio__(self) -> RadioSet:
        return self.get_widget_by_id(id="file_type", expect_type=RadioSet)


    def on_mount(self) -> None:
        #self.query_one(LoadingIndicator).display = True
        self.set_focus(self.__main_table__())
        file_tree = self.__files_tree__()
        base_info = self.explorer.file_info(self.base_path)
        root_label =  self.__file_label__(base_info)
        file_tree.root.set_label(root_label)
        file_tree.root.data = base_info
        self.__refresh_top_status__("")
        self.action_reload_table()


    @staticmethod
    def __file_type__(index: int) -> str:
        match index:
            case 0: type = "PARQUET"
            case 1: type = "JSON"
            case 2: type = "CSV"
            case _: type = "UNKNOWN"
        return type


    def __get_file_type__(self) -> str:
        return self.__file_type__(self.__file_type_radio__().pressed_index)


    def on_radio_set_changed(self, event: RadioSet.Changed) -> None:
        if event.radio_set.id=="file_type":
            self.file_type = self.__get_file_type__()
            self.notify(f"{self.file_type} selected")


    def load_data(self) -> None:
        data_table = self.__main_table__()
        data_table.loading = True
        data_table.clear(columns=True)
        data_table.add_columns(*self.tab.column_names)
        data_table.add_rows(self.tab.row_values)
        data_table.loading = False
        self.action_refresh_table()


    @staticmethod
    def __add_subfields_to_tree(field_info: {}, node: TreeNode):
        if field_info["kind"] == "simple":
            node.add_leaf(f"{field_info['name']} ({field_info['type']})", data=field_info)
        else:
            added = node.add(f"{field_info['name']} ({field_info['type']})", data=field_info)
            for subfield in field_info["subfields"]:
                DataApp.__add_subfields_to_tree(subfield, added)
            added.expand()


    def load_structure(self) -> None:
        tree: Tree = self.__struct_tree__()
        tree.clear()
        tree.show_root = False
        tree.auto_expand = True

        for f in self.tab.schema_tree:
            self.__add_subfields_to_tree(f, tree.root)

    @work
    async def action_reload_table(self) -> None:
        self.notify("refreshing...")
        self.tab = self.orig_tab
        self.load_data()
        self.load_structure()
        #self.query_one(LoadingIndicator).display = False


    def action_refresh_table(self) -> None:
        # experimental - refresh by getting table out of focus and focus again, no other method worked (refresh etc.)
        self.set_focus(self.__struct_tree__())
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
        current_file = self.__files_tree__().cursor_node
        if current_file is None:
            self.notify(f"No file/directory selected")
            return

        if not current_file.data["is_dir"]:
            self.notify(f"File (not directory) is selected {current_file.data}")
            return

        self.notify(f"Refreshing {current_file.data['name']}") # {current_file.data}")
        path = current_file.data["full_path"]
        dir_contents = self.explorer.read_directory(path)
        current_file.remove_children()
        for f in dir_contents:
            label = self.__file_label__(f)
            current_file.add(label=label, data=f)

        current_file.expand()


    def action_read_file(self) -> None:
        #self.query_one(LoadingIndicator).display = True
        current_file = self.__files_tree__().cursor_node
        if current_file is None:
            self.notify(f"No file/directory selected")
            return

        self.read_file(current_file.data["full_path"])



    @work
    async def read_file(self, path: str) -> None:
        self.notify(f"Reading file: {path}")

        await self.push_screen(BusyScreen())
        await asyncio.sleep(1)
        self.refresh()
        tab = self.explorer.read_file(self.file_type, path)
        await self.pop_screen()

        #TODO: improve this very simplistic approach to error handling
        if tab is None:
            self.notify(f"Error occured reading file: {path}")
        else:
            self.orig_tab = tab
            self.__refresh_top_status__(path)
            self.action_reload_table()


    def __refresh_top_status__(self, path: str) -> None:
        status = self.__top_status__()
        path_fragment = path if len(path) < len(self.base_path) else f"{path[len(self.base_path)-1:]}"
        status_txt = (f"Base path: {self.base_path} | Loaded file: {path_fragment}\n" +
                      f"Rows: {self.explorer.params['take_rows']} | Files: {self.explorer.params['file_limit']}")
        status.update(status_txt)


    def __selected_cell_info__(self) -> (int, int, {}):
        main_tab = self.__main_table__()
        x = main_tab.cursor_column
        y = main_tab.cursor_row
        column, cell = self.tab.select(x,y)
        return x, y, column, cell


    @on(DataTable.CellHighlighted, "#main_table")
    def cell_highlighted(self, event: DataTable.CellHighlighted):
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
    def cell_selected(self, event: DataTable.CellSelected):
        x, y, _, _ = self.__selected_cell_info__()
        embedded_tab = extract_embedded_table(self.tab, x, y, expand_structs = True)
        if embedded_tab is None:
            self.notify("no further details available")
        else:
            self.notify(f"drilling into details: {len(embedded_tab.row_values)} row(s)")
            self.tab = embedded_tab
            self.load_data()


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

# if __name__ == "__main__":
#     app = DataApp()
#     app.run()
