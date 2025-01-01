from textual.app import ComposeResult
from textual.containers import Vertical
from textual.screen import ModalScreen
from textual.widgets import Static


class BusyScreen(ModalScreen):
    def compose(self) -> ComposeResult:

        with Vertical(id = "busy_dialog"):
            yield Static(content=" Waiting for spark session... ", id="busy_label")
