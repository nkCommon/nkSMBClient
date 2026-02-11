import smbclient
import os
import io
import pandas as pd
from dataclasses import dataclass
from datetime import datetime
from io import BytesIO
from typing import Any, Collection


@dataclass
class FileInfo:
    """File or directory info from list_files(include_metadata=True)."""
    name: str  # Base name only (no folder path)
    folder: str  # Folder path relative to list_files path ("" if at root)
    size: int | None
    creation_time: datetime | None
    last_modified: datetime | None
    is_dir: bool | None = None  # Set when files_only=False
    full_share_path: str | None = None

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, FileInfo) :
            return False
            
        return self.name == other.name and self.folder == other.folder and self.size == other.size and self.last_modified == other.last_modified
    
    def __ne__(self, other: Any) -> bool:
        return not self.__eq__(other)
    
    def __hash__(self) -> int:
        return hash((self.name, self.folder, self.size, self.last_modified))

class nkSMBClient:
    def __init__(self, server, share, username, password):
        self.server = server
        self.share = share
        self.username = username
        self.password = password
        self.files = None

        smbclient.ClientConfig(
            username=self.username,
            password=self.password
        )

    def _smb_path(self, path_in_share: str) -> str:
        return fr"\\{self.server}\{self.share}\{path_in_share}"

    def list_files(
        self,
        path_in_share: str,
        *,
        files_only: bool = False,
        recursive: bool = False,
        max_depth: int | None = None,
        exclude_names: Collection[str] | None = (".DS_Store",),
        include_metadata: bool = False,
    ):
        """
        List names in a directory on the share.

        Args:
            path_in_share: Path relative to the share (e.g. "Tools\\testdata").
            files_only: If True, return only file names (exclude directories).
            recursive: If True, walk subdirectories and return paths relative to path_in_share.
            max_depth: When recursive=True, stop after this many subfolder levels (1 = root + first
                subfolders only, no deeper). None = unlimited.
            exclude_names: Names to exclude from the list (e.g. .DS_Store). Use () to include all.
            include_metadata: If True, return a list of FileInfo with name (base name only),
                folder (path relative to list path), size, creation_time, last_modified
                (and is_dir when not files_only).

        Returns:
            List of names (or relative paths when recursive=True), or list of
            FileInfo when include_metadata=True.
        """
        try:
            smb_path = self._smb_path(path_in_share)
            exclude = set(exclude_names or ())

            def _should_include(name: str) -> bool:
                return name not in exclude and name not in (".", "..")

            def _split_folder_name(rel_path: str, nearest_folder: bool = False) -> tuple[str, str]:
                """Split 'folder1\\sub\\file.txt' into ('folder1\\sub', 'file.txt')."""
                if nearest_folder:
                    return rel_path.split("\\")[-2], rel_path.split("\\")[-1]
                if "\\" in rel_path:
                    idx = rel_path.rfind("\\")
                    return rel_path[:idx], rel_path[idx + 1:]
                return "", rel_path
            
            def _get_full_path_name(rel_path: str) -> str:
                if "\\" in rel_path:
                    idx = rel_path.rfind("\\")
                    full_path_name = rel_path[:idx].replace(f"\\\\{self.server}\\{self.share}\\", "")
                    return full_path_name
                return ""
            
            def _entry_info(entry: Any, rel_name: str) -> FileInfo:
                folder, name = _split_folder_name(entry.path, nearest_folder=True)#rel_name)
                full_share_path = _get_full_path_name(entry.path)
                st = entry.stat()
                return FileInfo(
                    name=name,
                    folder=folder,
                    size=getattr(st, "st_size", None),
                    creation_time=datetime.fromtimestamp(st.st_ctime) if st.st_ctime else None,
                    last_modified=datetime.fromtimestamp(st.st_mtime) if st.st_mtime else None,
                    is_dir=entry.is_dir() if not files_only else None,
                    full_share_path=full_share_path,
                )

            if not recursive:
                if include_metadata:
                    result = []
                    for entry in smbclient.scandir(smb_path):
                        if not _should_include(entry.name):
                            continue
                        if files_only and not entry.is_file():
                            continue
                        result.append(_entry_info(entry, entry.name))
                    self.files = result
                    return self.files
                if files_only:
                    result = []
                    for entry in smbclient.scandir(smb_path):
                        if _should_include(entry.name) and entry.is_file():
                            result.append(entry.name)
                    self.files = result
                    return self.files
                self.files = [n for n in smbclient.listdir(smb_path) if _should_include(n)]
                return self.files

            # Recursive: walk and collect relative paths (or metadata)
            result = []

            def _walk(prefix: str, smb_dir: str, depth: int = 0) -> None:
                for entry in smbclient.scandir(smb_dir):
                    if not _should_include(entry.name):
                        continue
                    rel = f"{prefix}{entry.name}" if prefix else entry.name
                    if entry.is_file():
                        if include_metadata:
                            result.append(_entry_info(entry, rel))
                        else:
                            result.append(rel)
                    elif entry.is_dir():
                        if include_metadata and not files_only:
                            result.append(_entry_info(entry, rel))
                        if max_depth is None or depth < max_depth:
                            sub_smb = f"{smb_dir}\\{entry.name}"
                            sub_prefix = f"{rel}\\"
                            _walk(sub_prefix, sub_smb, depth + 1)

            _walk("", smb_path)
            self.files = result
            return self.files
        except Exception as e:
            return []
        
    def list_folders(
        self,
        path_in_share: str,
        *,
        recursive: bool = False,
        max_depth: int | None = None,
        exclude_names: Collection[str] | None = (".DS_Store",),
        include_metadata: bool = False,
    ):
        """
        List folder (directory) names in a directory on the share.

        Args:
            path_in_share: Path relative to the share (e.g. "Tools\\testdata").
            recursive: If True, walk subdirectories and return folder paths relative to path_in_share.
            max_depth: When recursive=True, stop after this many subfolder levels (1 = root + first
                subfolders only, no deeper). None = unlimited.
            exclude_names: Names to exclude from the list. Use () to include all.
            include_metadata: If True, return a list of FileInfo with name, folder, size,
                creation_time, last_modified, is_dir=True.

        Returns:
            List of folder names (or relative paths when recursive=True), or list of
            FileInfo when include_metadata=True.
        """
        smb_path = self._smb_path(path_in_share)
        exclude = set(exclude_names or ())

        def _should_include(name: str) -> bool:
            return name not in exclude and name not in (".", "..")

        def _split_folder_name(rel_path: str) -> tuple[str, str]:
            if "\\" in rel_path:
                idx = rel_path.rfind("\\")
                return rel_path[:idx], rel_path[idx + 1:]
            return "", rel_path
        def _get_full_path_name(rel_path: str) -> str:
            if "\\" in rel_path:
                idx = rel_path.rfind("\\")
                full_path_name = rel_path[:idx].replace(f"\\\\{self.server}\\{self.share}\\", "")
                return full_path_name
            return ""
        def _entry_info(entry: Any, rel_name: str) -> FileInfo:
            folder, name = _split_folder_name(rel_name)
            full_share_path = _get_full_path_name(entry.path)
            st = entry.stat()
            return FileInfo(
                name=name,
                folder=folder,
                size=getattr(st, "st_size", None),
                creation_time=datetime.fromtimestamp(st.st_ctime) if st.st_ctime else None,
                last_modified=datetime.fromtimestamp(st.st_mtime) if st.st_mtime else None,
                is_dir=True,
                full_share_path=full_share_path,
            )

        if not recursive:
            if include_metadata:
                result = []
                for entry in smbclient.scandir(smb_path):
                    if not _should_include(entry.name) or not entry.is_dir():
                        continue
                    result.append(_entry_info(entry, entry.name))
                return result
            return [
                entry.name
                for entry in smbclient.scandir(smb_path)
                if _should_include(entry.name) and entry.is_dir()
            ]

        result = []

        def _walk(prefix: str, smb_dir: str, depth: int = 0) -> None:
            for entry in smbclient.scandir(smb_dir):
                if not _should_include(entry.name) or not entry.is_dir():
                    continue
                rel = f"{prefix}{entry.name}" if prefix else entry.name
                if include_metadata:
                    result.append(_entry_info(entry, rel))
                else:
                    result.append(rel)
                if max_depth is None or depth < max_depth:
                    sub_smb = f"{smb_dir}\\{entry.name}"
                    sub_prefix = f"{rel}\\"
                    _walk(sub_prefix, sub_smb, depth + 1)

        _walk("", smb_path)
        return result

    def read_bytes(self, file_path_in_share: str) -> bytes:
        smb_path = self._smb_path(file_path_in_share)
        with smbclient.open_file(smb_path, mode="rb") as f:
            return f.read()

    def read_text(self, file_path_in_share: str, encoding="utf-8") -> str:
        return self.read_bytes(file_path_in_share).decode(encoding)

    def read_csv(self, file_path_in_share: str, **kwargs) -> pd.DataFrame:
        smb_path = self._smb_path(file_path_in_share)
        with smbclient.open_file(smb_path, mode="rb") as f:
            return pd.read_csv(f, **kwargs)
    
    ## TODO: Implement create_folders_if_not_exist
    def move_file(
        self, 
        source_file_path_in_share:str, 
        target_file_path_in_share:str
        ):
        
        if source_file_path_in_share is None:
            raise Exception("source filename missing")
        if target_file_path_in_share is None:
            raise Exception("target filename missing")

        source_smb_path = self._smb_path(source_file_path_in_share)
        target_smb_path = self._smb_path(target_file_path_in_share)


        smbclient.rename(source_smb_path, target_smb_path)

    def make_dirs(self, path_in_share: str):
        smb_path = self._smb_path(path_in_share)
        smbclient.mkdir(smb_path)

    def read_excel_from_smb(
        self,
        path_in_share: str,
        sheet_name=0,
        **pd_kwargs,
    ) -> pd.DataFrame:
        """
        Read an Excel file from an SMB share into a pandas DataFrame.
        path_in_share example: 'revenue/data/file.xlsx'
        """
        # smbclient.register_session(server, username=username, password=password)
        smb_path = self._smb_path(path_in_share)

        with smbclient.open_file(smb_path, mode="rb") as f:
            data = f.read()

        return pd.read_excel(BytesIO(data), sheet_name=sheet_name, **pd_kwargs)        
    
    def download_file(self, file_path_in_share: str, local_file_path: str):
        smb_path = self._smb_path(file_path_in_share)
        with smbclient.open_file(smb_path, mode="rb") as f:
            data = f.read()

        with open(local_file_path, "wb") as f:
            f.write(data)
        
    def upload_file(self, local_file: str, smb_file_path_in_share: str):
        smb_path = self._smb_path(smb_file_path_in_share)
        with open(local_file, "rb") as f:
            data = f.read()

        with smbclient.open_file(smb_path, mode="wb") as f:
            f.write(data)
    
    def delete_file(self, smb_file_path_in_share: str):
        smb_path = self._smb_path(smb_file_path_in_share)
        smbclient.remove(smb_path)
        