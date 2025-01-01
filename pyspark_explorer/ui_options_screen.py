from textual import on
from textual.app import ComposeResult
from textual.containers import VerticalScroll, Horizontal
from textual.screen import ModalScreen
from textual.widget import Widget
from textual.widgets import Label, Input, Button, Checkbox


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

    def __init__(self, input_label: str, input_value) -> None:
        super().__init__(id=self.widget_id(input_label))
        self.input_label = input_label
        self.input_value = input_value

    @staticmethod
    def widget_id(label: str) -> str:
        return f"option_id_{label}"


    def compose(self) -> ComposeResult:
        yield Label(self.input_label)
        yield Input(value=str(self.input_value))


    def get_value(self):
        return self.query_one(Input).value


class OneOptionBool(OneOption):
    """An input with a label."""

    DEFAULT_CSS = """
    OneOption CheckBox {
        width: 1fr;
    }
    """

    def __init__(self, input_label: str, input_value: bool) -> None:
        super().__init__(input_label, input_value)

    def compose(self) -> ComposeResult:
        yield Label(self.input_label)
        yield Checkbox("", self.input_value)

    def get_value(self):
        return self.query_one(Checkbox).value


class OneOptionNum(OneOption):
    def __init__(self, input_label: str, input_value: int) -> None:
        super().__init__(input_label, input_value)


    def compose(self) -> ComposeResult:
        yield Label(self.input_label)
        yield Input(value=str(self.input_value), type="integer")


    def get_value(self):
        return int(self.query_one(Input).value)


class OptionsScreen(ModalScreen[dict]):

    def __init__(self, options: dict) -> None:
        self.init_options = options
        super().__init__()

    def compose(self) -> ComposeResult:
        with VerticalScroll(id = "options_container"):
            yield Label("Options", id="options_header")
            for key, value in self.init_options.items():
                if isinstance(value, bool):
                    yield OneOptionBool(key, value)
                elif isinstance(value, int):
                    yield OneOptionNum(key, value)
                else:
                    yield OneOption(key, value)

            with Horizontal(id = "options_buttons_container"):
                yield Button("Ok", id="options_ok", variant="success")
                yield Button("Cancel", id="options_cancel", variant="default")


    def __read_options__(self) -> dict:
        opts = {}
        for key, value in self.init_options.items():
            component = self.get_widget_by_id(OneOption.widget_id(key))
            opts[key] = component.get_value() if component is not None else value

        return opts


    @on(Button.Pressed, "#options_ok")
    def handle_yes(self) -> None:
        self.dismiss(self.__read_options__())


    @on(Button.Pressed, "#options_cancel")
    def handle_no(self) -> None:
        self.dismiss(None)
