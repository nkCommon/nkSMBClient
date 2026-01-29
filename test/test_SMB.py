import unittest
from datetime import datetime
from NKSMBClient.src.nkSMBClient import nkSMBClient
import os
from dotenv import load_dotenv

# load_dotenv()

class TestSMB(unittest.TestCase):
    def setUp(self):
        self.user = os.getenv("SMB_USERNAME")
        self.pwd = os.getenv("SMB_PASSWORD")
        self.server = os.getenv("SMB_SERVER")
        self.share = os.getenv("SMB_SHARE")
        self.path_in_share = r"Tools\testdata"
        self.path_in_share_moved = r"Tools\testdata_moved"
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
    ## read files
    def test_get_files(self):
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
        
