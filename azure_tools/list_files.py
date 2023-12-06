from pathlib import Path
from sys import stderr
from typing import Generator
from typing import Optional

from azure.storage.blob import BlobProperties
from azure.storage.blob import ContainerClient
from azure.storage.fileshare import DirectoryProperties
from azure.storage.fileshare import FileProperties
from azure.storage.fileshare import ShareClient
from click import Path as ClickPath
from click import argument
from click import group
from click import option

from azure_tools.functions import file_checksum


def list_fileshare_files(
    client: ShareClient, directory_name: Optional[str], output_dir: Path
) -> Generator[tuple[str, None], None, None]:
    output_files: Path = output_dir / (directory_name or "") / "files.txt"
    output_files.parent.mkdir(parents=True, exist_ok=True)
    output_files.unlink(missing_ok=True)

    item: DirectoryProperties | FileProperties

    for item in client.list_directories_and_files(directory_name=directory_name, include_extended_info=True):
        if item.is_directory:
            yield from list_fileshare_files(
                client, f"{f'{directory_name}/' if directory_name else ''}{item.name}", output_dir
            )
        else:
            print(item)
            input()
            filename: str = f"{f'/{directory_name}' if directory_name else ''}/{item.name}"
            with output_files.open("a", encoding="utf-8") as fh:
                fh.write(filename + "\n")
            yield filename, None


def list_container_files(
    client: ContainerClient, name_starts_with: Optional[str], output_dir: Path
) -> Generator[tuple[str, bytearray], None, None]:
    output_files_prev: Optional[Path] = None
    item: BlobProperties

    for item in client.list_blobs(name_starts_with=name_starts_with):
        item_path: Path = output_dir / item.name
        output_files: Path = item_path.parent / "files.txt"
        output_files.parent.mkdir(parents=True, exist_ok=True)
        filename: str = "/" + item.name

        if output_files != output_files_prev:
            output_files.unlink(missing_ok=True)

        output_files_prev = output_files

        with output_files.open("a", encoding="utf-8") as fh:
            fh.write(filename + "\t" + item.content_settings.content_md5.hex() + "\n")

        yield filename, item.content_settings.content_md5


def list_files(files: Generator[tuple[str, Optional[bytearray]], None, None], check_folder: Optional[Path]):
    missing_files: list[str] = []
    checksum_error_files: list[list] = []

    for i, [filename, checksum] in enumerate(files, 1):
        print(i, filename)
        if check_folder:
            file_path = check_folder.joinpath(filename)
            if not file_path.is_file():
                print(f"missing {filename}", file=stderr)
                missing_files.append(filename)
            elif checksum and file_checksum(file_path) != checksum:
                print(f"checksum mismatch {filename}", file=stderr)
                checksum_error_files.append(filename)

    if missing_files:
        print(f"\nFound {len(missing_files)} missing files:")
        for filename in missing_files:
            print(filename)

    if checksum_error_files:
        print(f"\nFound {len(checksum_error_files)} with differing checksums:")
        for filename in checksum_error_files:
            print(filename)


@group("ls", no_args_is_help=True)
def app_list_files():
    pass


@app_list_files.command("fileshare", no_args_is_help=True)
@argument("connection_string")
@argument("name", metavar="SHARE_NAME")
@option("--directory-name", type=str, default=None)
@option("--check-folder", type=ClickPath(exists=True, file_okay=False, resolve_path=True, path_type=Path), default=None)
@option(
    "--output-folder", type=ClickPath(exists=True, file_okay=False, resolve_path=True, path_type=Path), default=None
)
def app_list_files_fileshare(
    connection_string: str,
    name: str,
    directory_name: Optional[str],
    check_folder: Optional[Path],
    output_folder: Optional[Path],
):
    output_folder = output_folder or Path.cwd()

    fileshare_client = ShareClient.from_connection_string(connection_string, name)
    output_folder = output_folder / fileshare_client.account_name / fileshare_client.share_name

    files = list_fileshare_files(fileshare_client, directory_name, output_folder)

    list_files(files, check_folder)


@app_list_files.command("blob", no_args_is_help=True)
@argument("connection_string")
@argument("name", metavar="CONTAINER_NAME")
@option("--name-starts-with", type=str, default=None)
@option("--check-folder", type=ClickPath(exists=True, file_okay=False, resolve_path=True, path_type=Path), default=None)
@option(
    "--output-folder", type=ClickPath(exists=True, file_okay=False, resolve_path=True, path_type=Path), default=None
)
def app_list_files_blob(
    connection_string: str,
    name: str,
    name_starts_with: Optional[str],
    check_folder: Optional[Path],
    output_folder: Optional[Path],
):
    output_folder = output_folder or Path.cwd()

    container_client = ContainerClient.from_connection_string(connection_string, name)
    output_folder = output_folder / container_client.account_name / container_client.container_name

    blobs = list_container_files(container_client, name_starts_with, output_folder)

    list_files(blobs, check_folder)
