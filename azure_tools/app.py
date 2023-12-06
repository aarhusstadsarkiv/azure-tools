from click import group

from .copy_files import app_copy_files
from .list_files import app_list_files


@group("azure-tools", no_args_is_help=True)
def app():
    pass


app.add_command(app_list_files)
app.add_command(app_copy_files)
