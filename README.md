# HDFS_command_factory
xavier adriaenssens
run HDFS commands from python

=============================================================================

    deleteCall = HDFSCommandFactory().command(
    
             typ="delete", 
             
             hdfs_directory="hdfs://127.0.0.1:9000/user/xavier/US_Stocks/*.*")
             
=============================================================================

=============================================================================

     QueryCall = HDFSCommandFactory().command(
     
             typ="query", 
             
             file =  "*.*,
             
             hdfs_directory="hdfs://127.0.0.1:9000/user/xavier/US_Stocks/")
             
=============================================================================

=============================================================================

     PutCall = HDFSCommandFactory().command(
     
             typ="put", 
     
             file =  "*.*,
             
             localdirectory = "/home/xavier/Documents/"
             
             hdfs_directory="hdfs://127.0.0.1:9000/user/xavier/US_Stocks")
             
 =============================================================================

 =============================================================================
 
     GetCall = HDFSCommandFactory().command(
     
             typ="get", 
             
             file =  "*.*,
             
             localdirectory = "/home/xavier/Documents/"
             
             hdfs_directory="hdfs://127.0.0.1:9000/user/xavier/US_Stocks/")
             
 ============================================================================= 
