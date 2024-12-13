from textual import on
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.widgets import Static, DataTable, Footer, Input, Label


class UI(App):
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
        yield Static("", id="top_status")
        #yield Static("2", id="top_mid")
        yield Input(id="path")
        yield Static("1234567890 1234567890", id="mid_status")
        yield DataTable(id="main_table")
        yield Static("4", id="bottom_left_status")
        yield Static("5", id="bottom_mid_status")
        yield Static("6", id="bottom_right_status")
        # yield Static(id="bottom_bar")
        yield Footer(show_command_palette=True)

    @on(Input.Submitted, "#path")
    def submit_path(self, event: Input.Submitted):
        self.notify(event.value)

    def on_mount(self) -> None:
        self.load_data()

    def load_data(self) -> None:
        data_table = self.get_widget_by_id(id="main_table", expect_type=DataTable)
        data_table.loading = True
        data_table.clear(columns=True)
        data_table.add_columns("a","b","c")
        data_table.add_rows([[1,2,3],[4,5,6],[7,8,9]])
        data_table.loading = False
    
    def action_reload_table(self) -> None:
        self.load_data()


    @on(DataTable.CellHighlighted, "#main_table")
    def cell_highlighted(self, event: DataTable.CellHighlighted):
        txt = f"{event.data_table.cursor_column+1}/{event.data_table.cursor_row+1}"
        # self.notify(txt)
        status = self.get_widget_by_id(id="top_status", expect_type=Static)
        status.update(txt)


    @on(DataTable.CellSelected, "#main_table")
    def cell_selected(self, event: DataTable.CellSelected):
        txt = f"{event.data_table.cursor_column+1}/{event.data_table.cursor_row+1}"
        self.notify(txt)


def main() -> None:
    UI().run()
    pass

if __name__ == "__main__":
    main()
