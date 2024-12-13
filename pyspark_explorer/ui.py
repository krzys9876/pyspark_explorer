from os import walk
from textual import on
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.widgets import DataTable, Header, Footer, Static, Input

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
        #path {
            column-span: 2;
            background: $secondary;
            height: 100%;
        }
        #mid_status {
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
        #Binding(key="j", action="down", description="Scroll down", show=False),
    ]

    def compose(self) -> ComposeResult:
        yield Header()
        yield Static("", id="top_status")
        yield Input(id="path")
        yield Static("1234567890 1234567890", id="mid_status")
        yield DataTable(id="main_table")
        yield Static("", id="bottom_left_status")
        yield Static("", id="bottom_mid_status")
        yield Static("6", id="bottom_right_status")
        yield Footer(show_command_palette=True)


    def __main_table__(self) -> DataTable:
        return self.get_widget_by_id(id="main_table", expect_type=DataTable)

    def __top_status__(self) -> Static:
        return self.get_widget_by_id(id="top_status", expect_type=Static)

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


    def action_reload_table(self) -> None:
        self.tab = self.orig_tab
        self.load_data()


    def __selected_cell_info__(self) -> (int, int, {}):
        main_tab = self.__main_table__()
        x = main_tab.cursor_column
        y = main_tab.cursor_row
        return x, y, self.tab.select(x,y)


    @on(DataTable.CellHighlighted, "#main_table")
    def cell_highlighted(self, event: DataTable.CellHighlighted):
        x, y, cell = self.__selected_cell_info__()
        pos_txt = f"{x+1}/{y+1}"
        top_status = self.__top_status__()
        top_status.update(pos_txt)
        cell_dv = cell["display_value"]
        dv_status = self.__bottom_mid_status__()
        dv_status.update(cell_dv)
        type_status = self.__bottom_left_status__()
        type_status.update(f"{cell["column"]["name"]}\n  {cell["column"]["type"]}\n  {cell["kind"]}")
        


    @on(DataTable.CellSelected, "#main_table")
    def cell_selected(self, event: DataTable.CellSelected):
        x, y, _ = self.__selected_cell_info__()
        embedded_tab = extract_embedded_table(self.tab, x, y)
        if embedded_tab is None:
            self.notify("no further details available")
        else:
            self.notify("TBD")


# if __name__ == "__main__":
#     app = DataApp()
#     app.run()
