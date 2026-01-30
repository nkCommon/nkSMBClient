import smbclient
import io
import pandas as pd
from io import BytesIO
from typing import Collection


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
        exclude_names: Collection[str] | None = (".DS_Store",),
    ):
        """
        List names in a directory on the share.

        Args:
            path_in_share: Path relative to the share (e.g. "Tools\\testdata").
            files_only: If True, return only file names (exclude directories).
            recursive: If True, walk subdirectories and return paths relative to path_in_share.
            exclude_names: Names to exclude from the list (e.g. .DS_Store). Use () to include all.

        Returns:
            List of names (or relative paths when recursive=True).
        """
        smb_path = self._smb_path(path_in_share)
        exclude = set(exclude_names or ())

        def _should_include(name: str) -> bool:
            return name not in exclude and name not in (".", "..")

        if not recursive:
            if files_only:
                result = []
                for entry in smbclient.scandir(smb_path):
                    if _should_include(entry.name) and entry.is_file():
                        result.append(entry.name)
                self.files = result
                return self.files
            self.files = [n for n in smbclient.listdir(smb_path) if _should_include(n)]
            return self.files

        # Recursive: walk and collect relative paths
        result = []

        def _walk(prefix: str, smb_dir: str) -> None:
            for entry in smbclient.scandir(smb_dir):
                if not _should_include(entry.name):
                    continue
                rel = f"{prefix}{entry.name}" if prefix else entry.name
                if entry.is_file():
                    result.append(rel)
                elif entry.is_dir():
                    sub_smb = f"{smb_dir}\\{entry.name}"
                    sub_prefix = f"{rel}\\"
                    _walk(sub_prefix, sub_smb)

        _walk("", smb_path)
        self.files = result
        return self.files

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
    
        
        
