from pathlib import Path
from typing import Union
import shutil
from blob_store.core import BlobPath, get_open_fn


def cp(src: Union[BlobPath, Path], dest: Union[BlobPath, Path]) -> None:
    if isinstance(src, BlobPath):
        src.cp(dest)
    else:
        with open(src, "rb") as fr:
            open_fn = get_open_fn(dest)
            with open_fn("wb") as fw:
                shutil.copyfileobj(fr, fw)
