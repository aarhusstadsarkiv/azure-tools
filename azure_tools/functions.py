from hashlib import md5
from pathlib import Path
from typing import Generator
from typing import Optional

from azure.storage.blob import BlobProperties
from azure.storage.blob import ContainerClient
from azure.storage.fileshare import DirectoryProperties
from azure.storage.fileshare import FileProperties
from azure.storage.fileshare import ShareClient


def file_checksum(path: Path) -> bytes:
    file_hash = md5()
    with path.open("rb") as f:
        chunk = f.read(2**20)
        while chunk:
            file_hash.update(chunk)
            chunk = f.read(2**20)
    return file_hash.digest()


def list_fileshare_files(
    client: ShareClient, directory_name: Optional[str]
) -> Generator[tuple[str, FileProperties], None, None]:
    item: DirectoryProperties | FileProperties

    for item in client.list_directories_and_files(directory_name=directory_name, include_extended_info=True):
        if item.is_directory:
            yield from list_fileshare_files(client, str(Path(directory_name or "", item.name)))
        else:
            yield directory_name or "", item


def list_container_files(
    client: ContainerClient, name_starts_with: Optional[str]
) -> Generator[BlobProperties, None, None]:
    yield from client.list_blobs(name_starts_with=name_starts_with)
