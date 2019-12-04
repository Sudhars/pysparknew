from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from datetime import date, timedelta


Path = spark._jvm.org.apache.hadoop.fs.Path
s3loc_dest_path = "/wdpr-apps-data/dclrms/latest3/tmp/Modifiedrecordstype2DF_new/data"
s3loc_src_path = "/wdpr-apps-data/dclrms/latest3/tmp/Modifiedrecordstype2DF/data"

print("%s" % s3loc_src_path)
print("%s" % s3loc_dest_path)
fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
print(datetime.now().strftime("%y/%m/%d %H:%M:%S"))
fs.delete(Path(s3loc_dest_path))
print(datetime.now().strftime("%y/%m/%d %H:%M:%S")) 
fu = spark._jvm.org.apache.hadoop.fs.FileUtil()
fu.copy(fs,Path(s3loc_src_path),fs,Path(s3loc_dest_path),False,spark._jsc.hadoopConfiguration())
print(datetime.now().strftime("%y/%m/%d %H:%M:%S"))