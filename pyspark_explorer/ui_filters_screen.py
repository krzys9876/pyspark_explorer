from textual import on
from textual.app import ComposeResult
from textual.containers import VerticalScroll, Horizontal
from textual.screen import ModalScreen
from textual.widgets import Label, Button, Input, Select


class FiltersScreen(ModalScreen[str]):

    def __init__(self, title: str, filters: [str]) -> None:
        self.init_filters = filters
        self.screen_title = title
        super().__init__()

    def compose(self) -> ComposeResult:
        with VerticalScroll(id = "filters_container"):
            yield Label(self.screen_title, id="filters_header")
            yield Input(id="filters_input")
            yield Select.from_values(self.init_filters, id="filters_select")
            with Horizontal(id = "filters_buttons_container"):
                yield Button("Ok", id="filters_ok", variant="success")
                yield Button("Cancel", id="filters_cancel", variant="default")


    def __input__(self) -> Input:
        return self.get_widget_by_id("filters_input", expect_type=Input)


    @on(Select.Changed)
    def select_changed(self, event: Select.Changed) -> None:
        if event.select.id != "filters_select":
            return
        self.__input__().value = str(event.value) if event.value!=Select.BLANK else ""


    @on(Button.Pressed, "#filters_ok")
    def handle_yes(self) -> None:
        self.dismiss(self.__input__().value)


    @on(Button.Pressed, "#filters_cancel")
    def handle_no(self) -> None:
        self.dismiss(None)
