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

	md5ColList = ['instnc_st_nm', 'asgn_strm_typ_cd', 'auto_cncl_in', 'bkng_dt', 'bkng_dts', 'cntry_cd', 'cntry_subdiv_cd', 'comctn_chan_nm', 'crncy_exchg_rt', 'crncy_id', 'crus_age_typ_nm'
		, 'crus_grp_id', 'crus_grp_typ_nm', 'crus_lylty_pgm_lvl_nm', 'crus_rt_ctgy_nm', 'cty_nm', 'drvd_addr_typ_nm', 'drvd_cntry_cd', 'drvd_cntry_subdiv_cd', 'drvd_cty_nm'
		, 'drvd_paid_lylty_pgm_lvl_nm', 'drvd_pstl_cd', 'drvd_res_cncl_dt', 'drvd_res_sts_nm', 'gst_age_nb', 'gst_crus_rt_ctgy_nm', 'gst_seq_nb', 'gst_sfb_nm', 'gst_typ_cd'
		, 'htl_req_id', 'invc_item_comm_pc', 'invc_item_pc', 'invc_item_tot_cr_am', 'invc_item_tot_deb_am', 'invc_item_typ_cd', 'invc_item_unt_cn', 'invc_item_unt_price_am'
		, 'pkg_id', 'prev_drvd_res_sts_nm', 'price_strm_typ_cd', 'pstl_cd', 'res_cncl_dt', 'res_dpst_in', 'res_eff_bkng_dts', 'res_mod_rt_ctgy_in', 'res_mod_sfb_in'
		, 'res_mod_strm_typ_in', 'res_mod_vyge_in', 'sfb_nm', 'ship_cd', 'ship_strm_nb', 'sls_chan_nm', 'src_sys_cntry_cd', 'src_sys_cntry_subdiv_cd', 'src_sys_price_pgm_cd'
		, 'src_sys_subtyp_1_cd', 'src_sys_subtyp_2_cd', 'src_sys_subtyp_3_cd', 'sys_gnrt_rec_in', 'trvl_agcy_cntry_subdiv_cd', 'trvl_agcy_cty_nm', 'trvl_agcy_id'
		, 'trvl_agcy_pstl_cd', 'trvl_agcy_src_sys_cntry_cd', 'trvl_agt_id', 'vyge_bkng_dt', 'vyge_id', 'vyge_strm_typ_bkng_dt']

	md5ColLists = [coalesce(col(c), lit('')) for c in md5ColList]

	
	tgt_pk = spark.read.orc('/wdpr-apps-data/dclrms/latest3/tmp/res_invc_item_pk_details/data')
			
	s_res_invc_item = spark.read.orc('/wdpr-apps-data/dclrms/latest3/tmp/s_res_invc_item/data')\
		.withColumn('md5Val',md5(concat(*md5ColLists)))
		
	TrueStginserts =  s_res_invc_item\
		.join(tgt_pk
			,(tgt_pk['res_id'] == s_res_invc_item['res_id'])
			&(tgt_pk['res_seq_id'] == s_res_invc_item['res_seq_id'])
			&(tgt_pk['gst_id'] == s_res_invc_item['gst_id'])
			&(tgt_pk['invc_item_id'] == s_res_invc_item['invc_item_id']), "left_outer")\
		.select(s_res_invc_item['*'],tgt_pk['md5Val'].alias('tgt_pk_md5val'),tgt_pk['vrsn_strt_dts'].alias('tgt_pk_vrsn_strt_dts'))\
		.filter((col('md5Val') <> col('tgt_pk_md5val')) | (col('tgt_pk_md5val').isNull() == True))\
		.withColumn("partition_dt",col('vrsn_strt_dts').cast("date"))
		

	#TrueStginserts.write.orc("/wdpr-apps-data/dclrms/latest3/tmp/res_invc_item_TrueStginserts/data")
	#TrueStginserts.show(5,False)
	affectedRecordsDt = TrueStginserts.select(col('tgt_pk_vrsn_strt_dts').cast("date").alias("partition_dt")).distinct()

	affectedRecordsDtlist = [row.partition_dt for row in affectedRecordsDt.collect()]
	#print type(affectedRecordsDtlist[0])

	#print affectedRecordsDtlist[0]

	data_ref = "res_invc_item_new"
	partition_col = "partition_dt"

	s3_loc = []

	for partition_dt in affectedRecordsDtlist:
		#s3_loc = s3_loc + ["%s/%s/%s/data/%s=%s" % (self.config_map["hdfs.root.directory"], "tmp", data_ref, partition_col, partition_dt)]
		if partition_dt != None:
			s3_loc = s3_loc + ["%s/%s/%s/data/%s=%s" % ('/wdpr-apps-data/dclrms/latest3', "tmp", data_ref, partition_col, partition_dt)]

	afffectedpartitions = spark.read.orc(s3_loc).drop('etl_proc_run_id', 'etl_targ_rec_set_id')
	
	afftectedpartitiondata = afffectedpartitions\
		.join(TrueStginserts
			,(afffectedpartitions['res_id'] == TrueStginserts['res_id'])
			&(afffectedpartitions['res_seq_id'] == TrueStginserts['res_seq_id'])
			&(afffectedpartitions['gst_id'] == TrueStginserts['gst_id'])
			&(afffectedpartitions['invc_item_id'] == TrueStginserts['invc_item_id'])
			&(afffectedpartitions['vrsn_strt_dts'] == TrueStginserts['tgt_pk_vrsn_strt_dts']), "left_outer")\
		.select(afffectedpartitions['*'],coalesce(TrueStginserts['tgt_pk_vrsn_strt_dts'],afffectedpartitions['vrsn_end_dts']).alias('updated_vrsn_end_dts'))\
		.drop("vrsn_end_dts")\
		.withColumnRenamed("updated_vrsn_end_dts", "vrsn_end_dts")\
		.withColumnRenamed("etl_chksum_am", "md5Val")\
		.withColumn("partition_dt",col('vrsn_strt_dts').cast("date"))
		
	
	TrueStginserts = TrueStginserts.drop("tgt_pk_md5val","tgt_pk_vrsn_strt_dts")
	
	#print TrueStginserts.columns
	
	#print afftectedpartitiondata.columns
	
	tgtwritedata = afftectedpartitiondata.unionByName(TrueStginserts)
	
	tgtwritedata.write.format("orc").mode("overwrite").partitionBy("partition_dt").save("/wdpr-apps-data/dclrms/latest3/tmp/res_invc_item_TrueStginserts/data")
	
