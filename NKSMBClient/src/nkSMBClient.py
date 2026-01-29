import smbclient
import io
import pandas as pd


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
    
    
    def move_file(self, source_file_path_in_share:str, target_file_path_in_share:str):
        if source_file_path_in_share is None:
            raise Exception("source filename missing")
        if target_file_path_in_share is None:
            raise Exception("target filename missing")

        source_smb_path = self._smb_path(source_file_path_in_share)
        target_smb_path = self._smb_path(target_file_path_in_share)
        
        smbclient.rename(source_smb_path, target_smb_path)
        
        
        
    # def get_first_file(self, read_data_path):
    #     files = self.list_files()
    #     if files:
    #         return files[0]
    #     return None
    
    
    # # Set up configuration and database settings (same as main)   
    # # Build SMB paths
    # smb_path = fr"\\{server}\{share}\{path_in_share}"
    # smb_read_data_path = fr"\\{server}\{share}\{read_data_path}"
    
    # # Ensure destination folder exists
    # try:
    #     smbclient.listdir(smb_read_data_path)
    #     print(f"Destination folder exists: {smb_read_data_path}")
    # except Exception:
    #     # Create destination folder if it doesn't exist
    #     print(f"Creating destination folder: {smb_read_data_path}")
    #     smbclient.makedirs(smb_read_data_path, exist_ok=True)
    
    # # List files in SMB share
    # print(f"Listing files in SMB share: {smb_path}")
    # files = smbclient.listdir(smb_path)
    # print(f"Found {len(files)} files")
    
    # # Create temporary directory for downloaded files
    # with tempfile.TemporaryDirectory() as temp_dir:
    #     # Filter for CSV files and process each one
    #     csv_files = [f for f in files if f.lower().endswith('.csv')]
    #     print(f"Found {len(csv_files)} CSV files to process")
        
    #     for filename in files:
    #         # Construct SMB path using backslashes (Windows UNC format)
    #         smb_file_path = fr"{smb_path}\{filename}"
    #         temp_file_path = os.path.join(temp_dir, filename)
            
    #         try:
    #             print(f"Downloading file: {smb_file_path}")
    #             # Download file from SMB share to temporary location
    #             with smbclient.open_file(smb_file_path, mode='rb') as smb_file:
    #                 with open(temp_file_path, 'wb') as local_file:
    #                     shutil.copyfileobj(smb_file, local_file)
                
    #             print(f"Processing file: {filename}")

    #             # Process the downloaded file (placeholder for actual processing logic)

    #             print(f"Successfully processed: {filename}")
                
    #             # Move file to read_data folder after successful processing
    #             destination_path = fr"{smb_read_data_path}\{filename}"
    #             try:
    #                 smbclient.rename(smb_file_path, destination_path)
    #                 print(f"Moved file to: {destination_path}")
    #             except Exception as move_error:
    #                 print(f"Warning: Failed to move file {filename} to read_data folder: {str(move_error)}")
                
    #         except Exception as e:
    #             print(f"Error processing {filename}: {str(e)}")
    #             rows_failed += 1
    

