CREATE EXTERNAL TABLE IF NOT EXISTS `lakehouse`.`machine_learning_curated`(
  `sensorreadingtime` bigint, 
  `serialnumber` string, 
  `distancefromobject` int,
  `user` string,
  `timestamp` bigint,
  `x` float,
  `y` float,
  `z` float
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe' 
WITH SERDEPROPERTIES ( 
  'case.insensitive' = 'TRUE', 
  'dots.in.keys' = 'FALSE', 
  'ignore.malformed.json' = 'FALSE', 
  'mapping' = 'TRUE'
) 
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://ggnp-udacity-bucket-2/machine_learning/curated/'
TBLPROPERTIES ('classification'='json', 'transient_lastDdlTime'='1690332095')
