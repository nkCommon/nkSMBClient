from pydoc import cli
import unittest
from datetime import datetime

from spnego import client
from NKSMBClient.src.nkSMBClient import nkSMBClient
import os
from dotenv import load_dotenv
import json
import hashlib
# load_dotenv()

class TestSMB(unittest.TestCase):
    def setUp(self):
        self.user = os.getenv("SMB_USERNAME")
        self.pwd = os.getenv("SMB_PASSWORD")
        self.server = os.getenv("SMB_SERVER")
        self.share = os.getenv("SMB_SHARE")
        self.path_in_share = r"Tools\testdata"
        self.path_in_share_privateGPTTest = r"privateGPT\RAGS"
        self.path_in_share_rules = r"Tools\test_rules"
        self.path_in_share_moved = r"Tools\testdata_moved"
        self.local_tmp_folder = "/Users/lakas/tmp"
        self.headers = str("point,timeofcreation,deliveryid,accountnumber,bookingdate,valuedate,currencycode,bookedamount,currencycodeorigin,bookedamountorigin,shortadvice,technical,startingbalance,balance,extendedadvice1,extendedadvice2,extendedadvice3,extendedadvice4,extendedadvice5,extendedadvice6,extendedadvice7,extendedadvice8,extendedadvice9,extendedadvice10,extendedadvice11,extendedadvice12,extendedadvice13,extendedadvice14,extendedadvice15,extendedadvice16,extendedadvice17,extendedadvice18,extendedadvice19,extendedadvice20,extendedadvice21,extendedadvice22,extendedadvice23,extendedadvice24,extendedadvice25,extendedadvice26,extendedadvice27,extendedadvice28,extendedadvice29,extendedadvice30,extendedadvice31,extendedadvice32,extendedadvice33,extendedadvice34,extendedadvice35,extendedadvice36,extendedadvice37,extendedadvice38,extendedadvice39,extendedadvice40,extendedadvice41,extendedadvice42,extendedadvice43,extendedadvice44,extendedadvice45,extendedadvice46,extendedadvice47,extendedadvice48,extendedadvice49").split(',')
        return super().setUp()

    def tearDown(self):
        return super().tearDown()

    ### Test cases below ###
    ## list files    
    def test_list_files(self):
        client = nkSMBClient(server=self.server, share=self.share, username=self.user, password=self.pwd)
        result = client.list_files(path_in_share=self.path_in_share)
        print(result)
        self.assertTrue(len(result)==3)
        result = client.list_files(path_in_share=self.path_in_share, include_metadata=True)
        print(result)
        self.assertTrue(len(result)==3)



    def test_list_files_privateGPTTest(self):
        client = nkSMBClient(server=self.server, share=self.share, username=self.user, password=self.pwd)
        # All files only, recursive, exclude .DS_Store (default)
        result = client.list_files(
            path_in_share=self.path_in_share_privateGPTTest,
            files_only=True,
            recursive=True,
        )
        print(result)
        self.assertTrue(len(result)==1)
        
        
       
    
    ## read files
    def test_read_csv_files(self):
        client = nkSMBClient(server=self.server, share=self.share, username=self.user, password=self.pwd)
        result = client.list_files(path_in_share=self.path_in_share)
        print(result)
        self.assertTrue(len(result)==3)
        
        
        print(fr"Reading csv file: {self.path_in_share}\{result[0]}")

        csv_dataframe = client.read_csv(
            file_path_in_share=fr"{self.path_in_share}\{result[0]}",
            delimiter=",",
            encoding="cp1252",
            names=self.headers, # override header row
            header=None,        # treat first row as data
            dtype={"bookingdate": str, "valuedate": str, "bookedamount": float}
            )
        
        print(len(csv_dataframe))        
        if result[0] == 'BETALINGSOB':
            self.assertTrue(len(csv_dataframe)== 5)
        
    def test_read_excel_files(self):
        client = nkSMBClient(server=self.server, share=self.share, username=self.user, password=self.pwd)
        result = client.list_files(path_in_share=self.path_in_share_rules)
        print(result)
        self.assertTrue(len(result)==4)
        
        excel_path = fr"{self.path_in_share_rules}\{result[0]}"
        
        print(f"Reading csv file: {excel_path}")
        
        first_row_is_header=0
        rule_columns = ["RULE_ID","SELECTOR"]
        dtype={"RULE_ID": int, "SELECTOR": str}
        
        excel_dataframe = client.read_excel_from_smb(
            path_in_share=excel_path,
            sheet_name="SQLRules_2",
            header = first_row_is_header,
            usecols=rule_columns,
            dtype=dtype
            )
        print(len(excel_dataframe))        
        if result[0] == 'BogføringsRegler for HovedKonto.xlsx':
            self.assertTrue(len(excel_dataframe)== 178)

    ## move files
    def test_move_files(self):
        client = nkSMBClient(server=self.server, share=self.share, username=self.user, password=self.pwd)
        result = client.list_files(path_in_share=self.path_in_share)
        print(result)
        self.assertTrue(len(result)==3)
        
        result = client.list_files(path_in_share=self.path_in_share_moved)
        self.assertTrue(len(result)==0)
        
        source_file = fr"{self.path_in_share}\BETALINGSOB"
        target_file = fr"{self.path_in_share_moved}\BETALINGSOB"
        client.move_file(source_file_path_in_share=source_file, target_file_path_in_share=target_file)
        
        result = client.list_files(path_in_share=self.path_in_share_moved)
        self.assertTrue(len(result)==1)
        result = client.list_files(path_in_share=self.path_in_share)
        self.assertTrue(len(result)==2)
        
        client.move_file(source_file_path_in_share=target_file, target_file_path_in_share=source_file)
        
        result = client.list_files(path_in_share=self.path_in_share_moved)
        self.assertTrue(len(result)==0)
        result = client.list_files(path_in_share=self.path_in_share)
        self.assertTrue(len(result)==3)

    def test_getfileinfo(self):
        client = nkSMBClient(server=self.server, share=self.share, username=self.user, password=self.pwd)
        result = client.list_files(path_in_share=self.path_in_share, files_only = True, include_metadata=True)

        print(result)
        self.assertTrue(len(result)==3)
        
        for fileinfo in result:
            print(fileinfo.name)
            print(fileinfo.size)
            print(fileinfo.creation_time)
            print(fileinfo.last_modified)
            print(fileinfo.is_dir)
            self.assertTrue(fileinfo.name is not None)
            
            if fileinfo.name == 'BETALINGSOB':
                self.assertTrue(fileinfo.size == 1913)
            if fileinfo.name == 'HOVEDKONTO':
                self.assertTrue(fileinfo.size == 22053)
            if fileinfo.name == 'NEMKONTO':
                self.assertTrue(fileinfo.size == 48284)

    def test_list_folders(self):
        client = nkSMBClient(server=self.server, share=self.share, username=self.user, password=self.pwd)
        folders = client.list_folders(path_in_share=self.path_in_share_privateGPTTest, recursive=True, max_depth=0)
        print(folders)
        self.assertTrue(len(folders)>0)

    def test_list_files_with_metadata_toplevel(self):
        client = nkSMBClient(server=self.server, share=self.share, username=self.user, password=self.pwd)
        files = client.list_files(path_in_share=self.path_in_share_privateGPTTest, files_only=True, recursive=True, include_metadata=True, max_depth=1)
        print(files)
        self.assertTrue(len(files)==1)
    def test_download_file(self):
        client = nkSMBClient(server=self.server, share=self.share, username=self.user, password=self.pwd)
        files = client.list_files(path_in_share=self.path_in_share_privateGPTTest, files_only=True, recursive=True, include_metadata=True, max_depth=1)
        print(files[0].name)
        print(files[0].folder)
        smb_file_path = fr"{self.path_in_share_privateGPTTest}\{files[0].folder}\{files[0].name}"
        print(smb_file_path)
        local_file_path = fr"{self.local_tmp_folder}/{files[0].name}"
        print(local_file_path)
        client.download_file(smb_file_path, local_file_path=local_file_path)
        self.assertTrue(os.path.exists(local_file_path))
        os.remove(local_file_path)
        
    def test_upload_delete_file(self):
        client = nkSMBClient(server=self.server, share=self.share, username=self.user, password=self.pwd)
        files = client.list_files(path_in_share=self.path_in_share, files_only=True, recursive=True, include_metadata=True, max_depth=1)
        before_upload_count = len(files)
        client.upload_file(local_file=fr"//Users/lakas/git/nkSMBClient/README.md", smb_file_path_in_share=fr"{self.path_in_share}\test_upload.md")
        files = client.list_files(path_in_share=self.path_in_share, files_only=True, recursive=True, include_metadata=True, max_depth=1)
        after_upload_count = len(files)
        print(len(files))
        
        self.assertTrue(after_upload_count == before_upload_count + 1)
        client.delete_file(smb_file_path_in_share=fr"{self.path_in_share}\test_upload.md")
        
        files = client.list_files(path_in_share=self.path_in_share, files_only=True, recursive=True, include_metadata=True, max_depth=1)
        after_delete_count = len(files)
        print(len(files))
        self.assertTrue(after_delete_count == before_upload_count)
    
    def test_dict_to_file(self):
        client = nkSMBClient(server=self.server, share=self.share, username=self.user, password=self.pwd)
        files = client.list_files(path_in_share=self.path_in_share, files_only=True, recursive=True, include_metadata=True, max_depth=1)
        before_upload_count = len(files)
        
        data = {
            "name": "test",
            "age": 30,
            "city": "Stege",
            "test_value": "øæåÆØÅ"
        }
        
        ## upload dict
        client.save_dict(data=data, path_in_share=fr"{self.path_in_share}\test_upload.json", create_folders_if_not_exist=True, indent=2, ensure_ascii=False)
        
        
        files = client.list_files(path_in_share=self.path_in_share, files_only=True, recursive=True, include_metadata=True, max_depth=1)
        after_upload_count = len(files)
        print(len(files))
        
        ## read dict
        read_data = client.read_dict(path_in_share=fr"{self.path_in_share}\test_upload.json", encoding="utf-8")
        
        print(read_data)
        print(data)

        data_string = json.dumps(data, sort_keys=True, ensure_ascii=False)
        hash_value_1 = hashlib.sha256(data_string.encode("utf-8")).hexdigest()
        
        data_string_read = json.dumps(read_data, sort_keys=True, ensure_ascii=False)
        hash_value_2 = hashlib.sha256(data_string_read.encode("utf-8")).hexdigest()

        print(hash_value_1)
        print(hash_value_2)
        
        self.assertTrue(hash_value_1 == hash_value_2)

        
        self.assertTrue(after_upload_count == before_upload_count + 1)
        client.delete_file(smb_file_path_in_share=fr"{self.path_in_share}\test_upload.json")
        
        files = client.list_files(path_in_share=self.path_in_share, files_only=True, recursive=True, include_metadata=True, max_depth=1)
        after_delete_count = len(files)
        print(len(files))
        self.assertTrue(after_delete_count == before_upload_count)