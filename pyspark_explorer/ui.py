from textual import on
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.widgets import DataTable, Header, Footer, Static, Input, Tree
from textual.widgets._tree import TreeNode

from pyspark_explorer.data_table import DataFrameTable, extract_embedded_table


class DataApp(App):

    def __init__(self, data: DataFrameTable, **kwargs):
        super(DataApp, self).__init__(**kwargs)
        self.orig_tab: DataFrameTable = data
        self.tab = self.orig_tab


    CSS = """
        Screen {
            layout: grid;
            grid-size: 3;
            grid-columns: 1fr 5fr 1fr;
            grid-rows: 3 5fr 5;
        }
        #top_status {
            background: $secondary;
            height: 100%;
        }
        #top_input {
            column-span: 2;
            background: $secondary;
            height: 100%;
        }
        #main_tree {
            height: 100%;
        }
        #main_table {
            column-span: 2;
        }
        #bottom_left_status {
            background: $boost;
            height: 100%;
        }
        #bottom_mid_status {
            background: $boost;
            height: 100%;
        }
        #bottom_right_status {
            background: $boost;
            height: 100%;
        }
        """

    BINDINGS = [
        Binding(key="^q", action="quit", description="Quit the app"),
        #Binding(key="question_mark", action="help", description="Show help screen", key_display="?"),
        Binding(key="r", action="reload_table", description="Reload table"),
        Binding(key="u", action="refresh_table", description="Refresh table", show=False),
        #Binding(key="j", action="down", description="Scroll down", show=False),
    ]

    def compose(self) -> ComposeResult:
        yield Header()
        yield Static("", id="top_status")
        yield Input(id="top_input")
        yield Tree("", id="main_tree")
        yield DataTable(id="main_table")
        yield Static("", id="bottom_left_status")
        yield Static("", id="bottom_mid_status")
        yield Static("", id="bottom_right_status")
        yield Footer(show_command_palette=True)


    def __main_table__(self) -> DataTable:
        return self.get_widget_by_id(id="main_table", expect_type=DataTable)

    def __top_status__(self) -> Static:
        return self.get_widget_by_id(id="top_status", expect_type=Static)

    def __main_tree__(self) -> Tree:
        return self.get_widget_by_id(id="main_tree", expect_type=Tree)

    def __top_input__(self) -> Input:
        return self.get_widget_by_id(id="top_input", expect_type=Input)

    def __bottom_left_status__(self) -> Static:
        return self.get_widget_by_id(id="bottom_left_status", expect_type=Static)

    def __bottom_mid_status__(self) -> Static:
        return self.get_widget_by_id(id="bottom_mid_status", expect_type=Static)

    def on_mount(self) -> None:
        self.set_focus(self.__main_table__())


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
            node.add_leaf(f"{field_info["name"]} ({field_info["type"]})", data=field_info)
        else:
            added = node.add(f"{field_info["name"]} ({field_info["type"]})", data=field_info)
            for subfield in field_info["subfields"]:
                DataApp.__add_subfields_to_tree(subfield, added)
            added.expand()


    def load_structure(self) -> None:
        tree: Tree = self.__main_tree__()
        tree.clear()
        tree.show_root = False
        tree.auto_expand = True

        for f in self.tab.schema_tree:
            self.__add_subfields_to_tree(f, tree.root)


    def action_reload_table(self) -> None:
        self.notify("refreshing...")
        self.tab = self.orig_tab
        self.load_data()
        self.load_structure()


    def action_refresh_table(self) -> None:
        # experimental - refresh by getting table out of focus and focus again, no other method worked (refresh etc.)
        self.set_focus(self.__main_tree__())
        self.set_focus(self.__main_table__())


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
        top_status = self.__top_status__()
        top_status.update(pos_txt)
        cell_dv = cell["display_value"]
        dv_status = self.__bottom_mid_status__()
        dv_status.update(cell_dv)
        type_status = self.__bottom_left_status__()
        status_text = f"{column["name"]}\n  {column["type"]}/{column["field_type"].typeName()}\n  {column["kind"]}"
        if column["type"]=="ArrayType":
            status_text = f"{status_text}\n  {len(cell["value"])} inner row(s)"

        type_status.update(status_text)

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


# if __name__ == "__main__":
#     app = DataApp()
#     app.run()
