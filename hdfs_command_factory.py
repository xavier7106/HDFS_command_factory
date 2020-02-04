#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Feb  3 16:44:46 2020

@author: xavier
"""

import subprocess

class hdfs_command(object):

   def __init__(self, **kwargs):
        self.file =  kwargs.get("file")
        self. hdfs_directory = kwargs.get("hdfs_directory")
        self.local_directory = kwargs.get("local_directory")
        self.run()
        self.output = ""
   def gethdfs_directory(self):
       return self.hdfs_directory
   def run(self):
       pass
   def getOutput(self):
       return self.Output

# =============================================================================
#     deleteCall = HDFSCommandFactory().command(
#             typ="delete", 
#             hdfs_directory="hdfs://127.0.0.1:9000/user/xavier/US_Stocks/*.*")
# =============================================================================
class delete(hdfs_command):
    def run(self):
        print("run delete")
        try:
            print(self.hdfs_directory)
            command = "hadoop fs -rm " + self.hdfs_directory+ "  "
            print("run delete command: "+command)
            (status, self.output) = subprocess.getstatusoutput(command)
            print(self.output)
        except ValueError:
            print(ValueError.__str__)
            print("error, delete_file request could not be completed")
        return self.output

# =============================================================================
#     QueryCall = HDFSCommandFactory().command(
#             typ="query", 
#             file =  "*.*,
#             hdfs_directory="hdfs://127.0.0.1:9000/user/xavier/US_Stocks/")
# ============================================================================= 
class Query(hdfs_command):
    def run(self):
        try:
            command = "hadoop fs -ls " + self.gethdfs_directory()+self.file +" "
            print("run query command: "+command)
            (status, self.output) = subprocess.getstatusoutput(command)
        except ValueError:
            print(ValueError.__str__)
            print("error,query_file request could not be completed")
        return self.output

# =============================================================================
#     PutCall = HDFSCommandFactory().command(
#             typ="put", 
#             file =  "*.*,
#             localdirectory = "/home/xavier/Documents/"
#             hdfs_directory="hdfs://127.0.0.1:9000/user/xavier/US_Stocks")
# ============================================================================= 
class Put(hdfs_command):
    def run(self):
         try:
             command = "hadoop fs -put " + self.localdirectory + self.file +" "+ self.directory
             print("run put command: "+command)
             (status, self.output) = subprocess.getstatusoutput(command)
         except ValueError:
             print(ValueError.__str__)
             print("error, get_file request could not be completed")
         return self.output

# =============================================================================
#     GetCall = HDFSCommandFactory().command(
#             typ="get", 
#             file =  "*.*,
#             localdirectory = "/home/xavier/Documents/"
#             hdfs_directory="hdfs://127.0.0.1:9000/user/xavier/US_Stocks/")
# ============================================================================= 
class Get(hdfs_command):
    def run(self):
         try:
             command = "hadoop fs -get " + self.hdfs_directory + self.file +" "+ self.localdirectory
             print("run get command: "+command)
             (status, self.output) = subprocess.getstatusoutput(command)
         except ValueError:
             print(ValueError.__str__)
             print("error, get_file request could not be completed")
     
         return self.output

class HDFSCommandFactory():
   def command(self, typ, **kwargs):
      print(typ.capitalize())
      targetclass = typ.capitalize()
      return globals()[targetclass](**kwargs)
  








    
