from textual.app import ComposeResult
from textual.containers import Vertical
from textual.screen import ModalScreen
from textual.widgets import Static


class BusyScreen(ModalScreen):
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
