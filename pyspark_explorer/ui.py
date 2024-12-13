from textual import events
from textual.app import App, ComposeResult
from textual.widgets import DataTable

from pyspark_explorer.data_table import DataFrameTable


class DataApp(App):

    def __init__(self, data: DataFrameTable, **kwargs):
        super(DataApp, self).__init__(**kwargs)
        self.tab: DataFrameTable = data


    CSS = """
    Screen {
        layout: grid;
        grid-size: 1;
    }
    DataTable {
        height: 1fr;
    }
    """

    def compose(self) -> ComposeResult:
        yield DataTable()

    def on_mount(self) -> None:
        pass
        # for data_table in self.query(DataTable):
        #     data_table.loading = True
        #     self.load_data(data_table)

    def load_data(self, data_table: DataTable) -> None:
        data_table.add_columns(*self.tab.column_names)
        data_table.add_rows(self.tab.row_values)
        data_table.loading = False

    def on_key(self, event: events.Key) -> None:
        if event.key=="r":
            for data_table in self.query(DataTable):
                data_table.loading = True
                self.load_data(data_table)


# if __name__ == "__main__":
#     app = DataApp()
#     app.run()
