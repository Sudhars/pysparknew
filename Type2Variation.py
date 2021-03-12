from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from datetime import date, timedelta
import sys,traceback
import os
from os import path
sys.path.append(((path.dirname(path.dirname(path.abspath(__file__))))))

if __name__ == '__main__':
	spark = SparkSession.builder.enableHiveSupport().getOrCreate()

	md5ColList = ['col1', 'col2', 'col3']

	md5ColLists = [coalesce(col(c), lit('')) for c in md5ColList]

	
	tgt_pk = spark.read.orc('source_path/data')
			
	s_invoice_item = spark.read.orc('/source_path/s_invoice/data')\
		.withColumn('md5Val',md5(concat(*md5ColLists)))
		
	TrueStginserts =  s_invoice_item\
		.join(tgt_pk
			,(tgt_pk['col1'] == s_invoice_item['col1'])
			&(tgt_pk['col2'] == s_invoice_item['col2'])
			&(tgt_pk['col3'] == s_invoice_item['col3']), "left_outer")\
		.select(s_res_invc_item['*'],tgt_pk['md5Val'].alias('tgt_pk_md5val'),tgt_pk['vrsn_strt_dts'].alias('tgt_pk_vrsn_strt_dts'))\
		.filter((col('md5Val') <> col('tgt_pk_md5val')) | (col('tgt_pk_md5val').isNull() == True))\
		.withColumn("partition_dt",col('vrsn_strt_dts').cast("date"))
		

	affectedRecordsDt = TrueStginserts.select(col('tgt_pk_vrsn_strt_dts').cast("date").alias("partition_dt")).distinct()

	affectedRecordsDtlist = [row.partition_dt for row in affectedRecordsDt.collect()]

	data_ref = "invoice_item_new"
	partition_col = "partition_dt"

	s3_loc = []

	for partition_dt in affectedRecordsDtlist:
		#s3_loc = s3_loc + ["%s/%s/%s/data/%s=%s" % (self.config_map["hdfs.root.directory"], "tmp", data_ref, partition_col, partition_dt)]
		if partition_dt != None:
			s3_loc = s3_loc + ["%s/%s/%s/data/%s=%s" % ('/sourcedata/latest3', "tmp", data_ref, partition_col, partition_dt)]

	afffectedpartitions = spark.read.orc(s3_loc).drop('additional_col1', 'additional_col2')
	
	afftectedpartitiondata = afffectedpartitions\
		.join(TrueStginserts
			,(afffectedpartitions['col1'] == TrueStginserts['col1'])
			&(afffectedpartitions['col2'] == TrueStginserts['col2'])
			&(afffectedpartitions['col3'] == TrueStginserts['col3'])
			&(afffectedpartitions['vrsn_strt_dts'] == TrueStginserts['tgt_pk_vrsn_strt_dts']), "left_outer")\
		.select(afffectedpartitions['*'],coalesce(TrueStginserts['tgt_pk_vrsn_strt_dts'],afffectedpartitions['vrsn_end_dts']).alias('updated_vrsn_end_dts'))\
		.drop("vrsn_end_dts")\
		.withColumnRenamed("updated_vrsn_end_dts", "vrsn_end_dts")\
		.withColumnRenamed("etl_chksum_am", "md5Val")\
		.withColumn("partition_dt",col('vrsn_strt_dts').cast("date"))
		
	
	TrueStginserts = TrueStginserts.drop("tgt_pk_md5val","tgt_pk_vrsn_strt_dts")
	
	tgtwritedata = afftectedpartitiondata.unionByName(TrueStginserts)
	
	tgtwritedata.write.format("orc").mode("overwrite").partitionBy("partition_dt").save("/targetpath/tmp/invoice_item_TrueStginserts/data")
	
