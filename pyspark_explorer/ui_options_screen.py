from textual import on
from textual.app import ComposeResult
from textual.containers import VerticalScroll, Horizontal
from textual.screen import ModalScreen
from textual.widget import Widget
from textual.widgets import Label, Input, Button


class OneOption(Widget):
    """An input with a label."""

    DEFAULT_CSS = """
    OneOption {
        layout: horizontal;
        height: auto;
    }
    OneOption Label {
        padding: 1;
        width: 20;
        text-align: right;
    }
    OneOption Input {
        width: 1fr;
    }
    """

    def __init__(self, input_label: str, input_value: str) -> None:
        self.input_label = input_label
        self.input_value = input_value
        super().__init__()

    def compose(self) -> ComposeResult:
        yield Label(self.input_label)
        yield Input(value=self.input_value)


class OptionsScreen(ModalScreen[dict]):

    def __init__(self, options: dict) -> None:
        self.init_options = options
        super().__init__()

    def compose(self) -> ComposeResult:
        with VerticalScroll(id = "options_container"):
            yield Label("Options", id="options_header")
            for key, value in self.init_options.items():
                yield OneOption(key, str(value))

            with Horizontal(id = "options_buttons_container"):
                yield Button("Ok", id="options_ok", variant="success")
                yield Button("Cancel", id="options_cancel", variant="default")


    @on(Button.Pressed, "#options_ok")
    def handle_yes(self) -> None:
        self.dismiss(self.init_options)


    @on(Button.Pressed, "#options_cancel")
    def handle_no(self) -> None:
        self.dismiss(None)
