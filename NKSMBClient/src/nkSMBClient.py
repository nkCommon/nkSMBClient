import smbclient
import io
import pandas as pd
from io import BytesIO


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

    def list_files(self, path_in_share: str):
        smb_path = self._smb_path(path_in_share)
        self.files = smbclient.listdir(smb_path)
        
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
    
        
        
