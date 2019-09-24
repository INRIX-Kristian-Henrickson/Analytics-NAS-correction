import pandas
import numpy as np
import pyspark
from pyspark.sql.functions import udf
from pyspark.sql.types import StructField, StructType, IntegerType, DoubleType, StringType
from pyspark.sql.window import Window
import pyarrow
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
import os

##--------------------------------	Inputs	----------------------------------------------------##

#	Need to specify the full path to the following:	
#	Process group definition files (@pgroup_location)
#	Road segment definition files (@rseg_location)
#	Holiday definition table with dates of holidays (@hday_location)
#	Road segment NAS 672, should be further partitioned by tilegroup using standard hadoop file names, e.g. tilegroup=000001 	(@nas_location)
#	Road segment interpolated point pair speed data, should be further partitioned by tilegroup (@int_location)

pgroup_location = 's3://analytics-nas-ad/process_groups/pgroup_defs.txt'
rseg_location	= 's3://analytics-nas-ad/road_segments/'
hday_location 	= 's3://analytics-nas-ad/holidays/referencedefs.holidays.bcp.gz'
nas_location 	= 's3://inrix.fusion.nas/1903/roadsegment-nas672/'
int_location 	= 's3://inrix.fusion.nas/1903/interpolated/'

#	Specify one or more years of holidays to include (as a list)
hday_years = None

#	Specify the max and min tilegroups to include (integers), this seems to change between map versions so should be specified
tg_min, tg_max = 1,1

#	Specify a temp s3 directory that can be used to save the result
#	You will need to reference this location in Step 2
scratch = 's3://analytics-nas-ad/scratch'

##-----------		Any inputs below here should be changed with caution  			------------##

#	Set up spark session and sql context
spark = SparkSession \
    .builder \
    .appName("NAS Anomaly detection step 1") \
    .config('spark.sql.session.timeZone', 'UTC') \
    .getOrCreate()
	
sqlContext = SQLContext(spark.sparkContext)
	
sqlContext.sql("set spark.sql.shuffle.partitions={}".format( 3500 ))

#	Define schemas for non-parquet input files
#	These seem to change every once in a while, probably worth a look to make sure they are the same
schema_segs = 'roadsegmentid INT, projected_travel_path INT, length_miles DOUBLE, avg_speed DOUBLE, std_dev DOUBLE, frc INT, process_group_id INT, speed_limit DOUBLE, region STRING'
schema_nas = 'segid INT, binid INT, speedmph INT, points INT, stdev DOUBLE, weekcount INT'
schema_interp = 'segid INT, speed INT, epoch INT'
schema_pg = 'pg_id INT, name STRING, utc_offset_hours INT, observes_dst INT, dst_region_id INT,holiday_region_id INT, country_code STRING, time_zone_info STRING, region STRING'
schema_holidays = 'holidaytypeid INT, holidayregionid INT, holidaydate STRING'

#	Read in the process group, road segment, and holiday reference data in that order
df_pg = spark.read.format("csv").option("header", "false").\
    schema(schema_pg).\
    option('delimiter','\t').\
    option('mode', 'DROPMALFORMED').\
    load(pgroup_location)
    
df_seg = spark.read.format("csv").option("header", "false").\
    schema(schema_segs).\
    option('delimiter','\t').\
    option('mode', 'DROPMALFORMED').\
    load(rseg_location)
    
df_hdays = spark.read.format("csv").option("header", "false").\
    schema(schema_holidays).\
    option('delimiter','\t').\
    option('mode', 'DROPMALFORMED').\
    load(hday_location) 
	
#	Create views from the first three dataframes
df_pg.createOrReplaceTempView("pg")
df_seg.createOrReplaceTempView("segs_pre")
df_hdays.createOrReplaceTempView("hdays_all")


#	THis step reduces the size of the holidays table to include only the year(s) of interest
#	Possibly not important, only done if a list of years (strings) is provided

if hday_years is None:
	df_hdays_filt = df_hdays.persist()
	df_hdays_filt.createOrReplaceTempView('hdays')
else:
	sqs = """
	SELECT hdays_all.*
	FROM hdays_all 
	WHERE LEFT(holidaydate, 4) IN ({})
	""".format(','.join(["'{}'".format(hy) for hy in hday_years]) )

	df_hdays_filt = spark.sql(sqs).persist()
	df_hdays_filt.createOrReplaceTempView('hdays')
	
for tgs in range(tg_min, tg_max + 1):
    tg = ('0000' + str(tgs))[-5:]
    print(tg)

    #   Read in the NAS and interpolated point pair data for a single tilegroup
    df = spark.read.parquet('{}/tilegroup={}'.format(nas_location, tg)) 
    df_interp = spark.read.parquet('{}/tilegroup={}'.format(int_location, tg)) 
    df_interp.createOrReplaceTempView("interp")
    df.createOrReplaceTempView("nas_all")

    #   Find the segments with stdev accross days greater than 10
    sqq = '''
    SELECT segmentId AS segid,
        MOD(binid,96) AS epoch,
        STD(mph) AS sd_mph
    FROM nas_all
    WHERE FLOOR(binid / 96.0) IN (1,2,3,4,5)
    GROUP BY segmentId, 
        MOD(binid,96)
    HAVING STD(mph) > 10.0
    '''

    dfseglist = spark.sql(sqq)
    dfseglist.createOrReplaceTempView("seglist_sd")

    sqq = '''
    WITH utg AS (
        SELECT segmentId AS segid,
            mph,
            percentile(mph,0.85) OVER (PARTITION BY segmentId) AS thresh
        FROM nas_all
    )
    SELECT segid,
        MAX(mph / thresh) AS max_thresh
    FROM utg
    GROUP BY segid
    HAVING MAX(mph / thresh) > 1.25
    '''

    dfseglist_perc = spark.sql(sqq)
    dfseglist_perc.createOrReplaceTempView("seglist_perc")
    
    sqs = """      
    SELECT DISTINCT segid
    FROM seglist_sd
      UNION 
    SELECT DISTINCT segid
    FROM seglist_perc
    """
      
    df_unique_segs = spark.sql(sqs)
    df_unique_segs.createOrReplaceTempView("unique_segs")
    
    #   Get the full NAS only for the segments identified in the previous step
    sqq = '''
    SELECT segid,
        nas.binid, 
        nas.mph AS speedmph, 
        nas.points, 
        nas.stddev AS stdev, 
        nas.weekcount,
        percentile(mph,0.85) OVER (PARTITION BY segid) AS limz
    FROM nas_all AS nas  JOIN unique_segs AS cte ON nas.segmentId = cte.segid
    WHERE binid < 7 * 96
    '''

    dfnas = spark.sql(sqq)
    dfnas.createOrReplaceTempView("nas")

    #   Get the timezone and country codes for the select segments from previous steps 
    sqq = '''
    WITH cte AS (
      SELECT DISTINCT segid
      FROM seglist_sd
      UNION 
      SELECT DISTINCT segid
      FROM seglist_perc
      )
    SELECT segs_pre.*,
        time_zone_info,
        country_code,
        holiday_region_id
    FROM segs_pre JOIN pg ON segs_pre.process_group_id = pg.pg_id
    JOIN cte ON cte.segid =  roadsegmentid
    '''

    df_seg_post = spark.sql(sqq)
    df_seg_post.createOrReplaceTempView("segs")

    #   Get the interpolated point pair data for the selected segments, along with timezone and country codes
    sql = """
    SELECT interp.segmentId AS segid,
        kph * 0.621371 AS speed_mph,
        minute * 60 AS epoch,
        CAST( from_utc_timestamp(from_unixtime(minute * 60), time_zone_info) AS DATE ) AS dates,
        time_zone_info,
        country_code,
        holiday_region_id,
        percentile(kph * 0.621371, 0.85) OVER (PARTITION BY interp.segmentId) AS thresh
    FROM  interp JOIN segs ON segs.roadsegmentid = interp.segmentId
    """

    df_perp = spark.sql(sql)    
    df_perp.createOrReplaceTempView("prp")

    #   Associate holidays and countries with interpolated point pairs in order to remove holiday dates from the point pair data
	#	ALso filter out extreme high values  (speed_mph / thresh <= 1.25) 
	#	Consider tuning the 1.25 parameter, did not spend a whole lot of time tuning it myself
	#	I tend to think that a number greater than 1.25 could be used for roads with lower ref speed
    sql = """
    SELECT prp.*,
        from_utc_timestamp(from_unixtime(epoch),time_zone_info) AS stamp
    FROM prp LEFT OUTER JOIN hdays ON dates = CAST(hdays.holidaydate AS DATE)
          AND hdays.holidayregionid = prp.holiday_region_id
    WHERE hdays.holidaytypeid IS NULL
    AND speed_mph / thresh <= 1.25
    """

    df_cte = spark.sql(sql)          
    df_cte.createOrReplaceTempView("cte")

    #   Make some changes in representation
	#	Here I am just generating some new columns that will be useful later, including for diagnostics at the end (e.g. binid, time of day (eopch), etc.)
    sql = """ 
    SELECT   segid,
        country_code,
        date_format(stamp,'u') AS weekday,
        CASE WHEN date_format(stamp,'u') = 6 OR date_format(stamp,'u') = 7 THEN 'weekend'
            ELSE 'weekday'
        END AS dws,
        FLOOR(hour(stamp) * 60.0 / 15) +  FLOOR(minute(stamp) / 15.0) AS epoch,
        (date_format(stamp,'u') % 7) * 96 + FLOOR(hour(stamp) * 60.0 / 15) +  FLOOR(minute(stamp) / 15.0) AS binid,
        speed_mph AS speed
    FROM cte 
    """

    df_interpols = spark.sql(sql)            
    df_interpols.createOrReplaceTempView("interpols")

    sql = """
    SELECT segid,
        country_code,
        weekday,
        dws,
        CAST( epoch AS INT ) AS epoch,
        CAST( binid AS INT ) AS binid,
        CAST( SUM(1.0 / (speed + 0.1)) AS DOUBLE ) AS speed,
        CAST( SUM(1.0 / (speed + 0.1)) AS DOUBLE ) AS speed_sm,
        CAST( COUNT(*) AS INT ) AS cnts,
        CAST(0 AS INT ) AS flags
    FROM interpols
    GROUP  BY segid,
        country_code,
        weekday,
        dws,
        CAST( epoch AS INT),
        CAST( binid AS INT )
    ORDER BY dws,
    CAST( epoch AS INT )
    """

    spark.sql(sql).repartition(1).write.parquet(scratch + '/source_corrections/tilegroup={}'.format(tg)) 
    dfnas.repartition(1).write.parquet(scratch + '/target_corrections/tilegroup={}'.format(tg)) 
