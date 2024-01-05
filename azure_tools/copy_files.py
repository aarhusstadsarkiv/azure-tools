from pathlib import Path
from typing import Optional

from azure.storage.blob import ContainerClient
from azure.storage.fileshare import ShareClient
from click import Path as PathClick
from click import argument
from click import group
from click import option

from .functions import file_checksum
from .functions import list_container_files
from .functions import list_fileshare_files
from .functions import save_chunks


@group("cp", no_args_is_help=True)
def app_copy_files():
    pass


@app_copy_files.command("fileshare", no_args_is_help=True)
@argument("connection_string")
@argument("name", metavar="SHARE_NAME")
@argument("dest", type=PathClick(file_okay=False, resolve_path=True, path_type=Path))
@option("--directory-name", type=str, default=None)
@option("--file-name", type=str, default=None)
@option("-u", "--update", is_flag=True, default=False)
def app_copy_files_fileshare(
    connection_string: str,
    name: str,
    dest: Path,
    directory_name: Optional[str],
    file_name: Optional[str],
    update: bool,
):
    fileshare_client = ShareClient.from_connection_string(connection_string, name)

    for path, file in list_fileshare_files(fileshare_client, directory_name, file_name):
        file_path = Path(path, file.name)
        print(file_path)

        dest_file_path = dest / file_path

        if update and dest_file_path.is_file() and dest_file_path.stat().st_size == file.size:
            continue
        else:
            dest_file_path.unlink(missing_ok=True)
            dest_file_path.parent.mkdir(parents=True, exist_ok=True)

        file_client = fileshare_client.get_file_client(str(file_path))
        file_stream = file_client.download_file(max_concurrency=4)

        save_chunks(file_stream.chunks(), dest_file_path)


@app_copy_files.command("blob", no_args_is_help=True)
@argument("connection_string")
@argument("name", metavar="CONTAINER_NAME")
@argument("dest", type=PathClick(file_okay=False, resolve_path=True, path_type=Path))
@option("--name-starts-with", type=str, default=None)
@option("-u", "--update", is_flag=True, default=False)
def app_copy_files_blob(connection_string: str, name: str, dest: Path, name_starts_with: str, update: bool):
    container_client = ContainerClient.from_connection_string(connection_string, name)

    for blob in list_container_files(container_client, name_starts_with):
        print(blob.name)

        dest_file_path = dest / blob.name

        if (
            update
            and dest_file_path.is_file()
            and dest_file_path.stat().st_size == blob.size
            and (
                not blob.content_settings.content_md5
                or blob.content_settings.content_md5 == file_checksum(dest_file_path)
            )
        ):
            continue
        else:
            dest_file_path.unlink(missing_ok=True)
            dest_file_path.parent.mkdir(parents=True, exist_ok=True)

        blob_client = container_client.get_blob_client(blob.name)
        blob_stream = blob_client.download_blob(max_concurrency=4)

        save_chunks(blob_stream.chunks(), dest_file_path)
