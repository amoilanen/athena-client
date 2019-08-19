create database athenatests;

create external table if not exists athenatests.cities(
  city string,
  population int
) row format serde 'org.apache.hadoop.hive.serde2.RegexSerDe'
with serdeproperties (
"input.regex" = "^(\\S+),(\\S+)$"
) location "s3://athena-tests-bucket/";