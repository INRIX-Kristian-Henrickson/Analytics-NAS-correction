import pandas
import numpy as np
import pyspark
from pyspark.sql.functions import udf
from pyspark.sql.types import StructField, StructType, IntegerType, DoubleType, StringType
from pyspark.sql.window import Window
import pyarrow
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession

##--------------------------------	Inputs	----------------------------------------------------##
#	Need to specify the s3 directory where the inputs are stored (nas and replacement values) as well as where the outputs will be placed
scratch = 's3://analytics-nas-ad/scratch'

#	Specify the tilegroup range to run
tg_min, tg_max = 1, 154

##-----------		Any inputs below here should be changed with caution  			------------##
#	Set up spark session and sql context
spark = SparkSession \
    .builder \
    .appName("NAS Anomaly detection step 2 (correction verbose)") \
    .config('spark.sql.session.timeZone', 'UTC') \
    .getOrCreate()
	
sqlContext = SQLContext(spark.sparkContext)
	
sqlContext.sql("set spark.sql.shuffle.partitions={}".format( 1500 ))


#   Function for dynamic moving average

def smooth_speed(binid, binlist, speedlist, countlist):
    wind_size = 45
    minpoints = 24
    for i in range(wind_size):
        dat = [(b, c, s) for b, c, s in zip(binlist,countlist,speedlist) if ( b <= (binid + i)) and (b >= binid - i) and (s > 0)]
        counts = sum([c for _,c,_ in dat])
        if( counts >= minpoints ):
            break
    try:
        ret_val = np.sum( [np.float( c ) for _, c, _ in dat] ) / np.sum( [np.float( s ) for _, _, s in dat] ) 
    except ZeroDivisionError:
        return(None)
        
    return(float( 1.0 * ret_val) )
	
spark.udf.register("speed_smooth", smooth_speed, DoubleType())

for tgs in range(tg_min, tg_max + 1):
    tg = ('0000' + str(tgs))[-5:]
    print(tg)
    
    #   Read in the NAS and interpolated point pair data
    df_source = spark.read.parquet('{}/source_corrections/tilegroup={}'.format(scratch,tg))
    df_source.createOrReplaceTempView('source')
    
    sqs = """
    WITH hcl AS (
        SELECT *,
            collect_list(binid) OVER (PARTITION BY segid ORDER BY binid ROWS BETWEEN 45 PRECEDING AND 45 FOLLOWING) AS binlist,
            collect_list(speed) OVER (PARTITION BY segid ORDER BY binid ROWS BETWEEN 45 PRECEDING AND 45 FOLLOWING) AS speedlist,
            collect_list(cnts) OVER (PARTITION BY segid ORDER BY binid ROWS BETWEEN 45 PRECEDING AND 45 FOLLOWING) AS countlist
        FROM source
    )
    SELECT segid,
        country_code,
        weekday,
        dws,
        epoch,
        binid,
        speed,
        speed_smooth(binid, binlist, speedlist, countlist) AS speed_sm,
        cnts,
        flags
    FROM hcl
    """
    
    df_intspeeds_final = spark.sql(sqs)
    df_intspeeds_final.createOrReplaceTempView("interpolated")
    
    
    df_target = spark.read.parquet('{}/target_corrections/tilegroup={}'.format(scratch,tg)) 
    df_target.createOrReplaceTempView("nas")


    sqq = """
    WITH cte AS (
        SELECT nas.segid,
                country_code,
                nas.binid,
                nas.speedmph AS nas_speed,
                nas.stdev,
                MAX(stdev) OVER (PARTITION BY nas.segid ORDER BY nas.binid ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS stdev_smooth,
                ints.weekday,
                ints.dws,
                ints.epoch,
                ints.speed * 1.0 / ints.cnts AS speed,
                ints.speed_sm,
                AVG(speed_sm) OVER (PARTITION BY dws, epoch) AS speed_epc,
                ints.cnts,
                ints.flags,
                speedmph / limz AS limz
        FROM interpolated AS ints JOIN nas 
        ON ints.segid = nas.segid
        AND ints.binid = nas.binid 
    ),prelim AS (
        SELECT segid,
            country_code,
            binid,
            epoch,
            stdev,
            stdev_smooth,
            weekday,
            dws,
            speed,
            speed_sm,
            speed_epc,
            cnts,
            nas_speed,
            AVG( 
                CASE WHEN (stdev_smooth > 20) AND cnts < 36 THEN speed_epc
                    WHEN (stdev_smooth > 10) AND (cnts < 24) THEN speed_sm
                    WHEN limz > 1.25 THEN speed_sm
                    ELSE nas_speed
                END 
                    ) OVER (PARTITION BY segid ORDER BY binid ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING) AS speed_smooth,
            CASE WHEN (stdev_smooth > 20) AND (cnts < 36) THEN 3
                WHEN (stdev_smooth > 10) AND (cnts < 24) THEN 2
                WHEN limz > 1.25 THEN 1
                ELSE 0
            END AS flags
        FROM cte 
    )
        SELECT segid,
            country_code,
            binid,
            epoch,
            stdev,
            stdev_smooth,
            weekday,
            dws,
            speed,
            nas_speed,
            cnts,
            speed_smooth,
            CASE WHEN MAX(flags) OVER ( PARTITION BY segid ORDER BY binid ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING ) > 0 THEN speed_smooth
                ELSE nas_speed
            END AS merged_speed,
            CASE WHEN MAX(flags) OVER ( PARTITION BY segid ORDER BY binid ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING ) > 0 AND flags == 0 THEN 4
                ELSE flags
            END AS flags
        FROM prelim 
    """
    
    spark.sql(sqq).repartition(100).write.parquet('{}/corrected_values_verbose/tilegroup={}'.format(scratch,tg))
