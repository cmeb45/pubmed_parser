import os
import re
from glob import glob
from datetime import datetime
import subprocess
import pubmed_parser as pp
from pyspark.sql import Row, SQLContext, Window
from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import rank, max, sum, desc
from utils import get_update_date

# Connect to S3 bucket
s3 = S3Connection('AWSAccessKeyId', 'AWSSecretKey', host='s3.amazonaws.com')
b = s3.get_bucket('bucket-name')
k = Key(b)

def update():
    """Download and update file"""
    save_file = os.path.join(save_dir, 'medline*_*_*_*.csv')
    file_list = list(filter(os.path.isdir, glob(save_file)))
    if file_list:
        d = re.search('[0-9]+_[0-9]+_[0-9]+', file_list[0]).group(0)
        date_file = datetime.strptime(d, '%Y_%m_%d')
        date_update = get_update_date(option='medline')
        # if update is newer
        is_update = date_update > date_file
        if is_update:
            print("MEDLINE update available!")
            subprocess.call(['rm', '-rf', os.path.join(save_dir, 'medline_*.csv')]) # remove
            subprocess.call(['rm', '-rf', download_dir])
            subprocess.call(['wget', 'ftp://ftp.ncbi.nlm.nih.gov/pubmed/updatefiles/*.xml.gz', '--directory', download_dir])
        else:
            print("No update available")
    else:
        print("Download MEDLINE for the first time")
        is_update = True
        date_update = get_update_date(option='medline')
        subprocess.call(['wget', 'ftp://ftp.ncbi.nlm.nih.gov/pubmed/baseline/*.xml.gz', '--directory', download_dir])
    return is_update, date_update

def parse_results_map(key):
    """Parse MEDLINE XML file"""
    # Extract name of file from key
    key_name = key.name.encode('utf-8')
    data_file = os.path.basename(key_name)
    # Download file from S3 bucket
    key.get_contents_to_filename(data_file)
    # Parse file
    temp = [Row(file_name=os.path.basename(data_file),**publication_dict) 
            for publication_dict in pp.parse_medline_xml(data_file)]
    # Delete file from local directory
    subprocess.call(['rm', '-rf', data_file])
    return temp

def parse_grant_map(key):
    """Parse Grant ID from MEDLINE XML file"""
    # Extract name of file from key
    key_name = key.name.encode('utf-8')
    data_file = os.path.basename(key_name)
    # Download file from S3 bucket
    key.get_contents_to_filename(data_file)
    # Parse file
    temp = pp.parse_medline_grant_id(data_file)
    # Delete file from local directory
    subprocess.call(['rm', '-rf', data_file])
    return temp

def process_file(date_update):
    """Process downloaded MEDLINE folder to csv file"""
    print("Process MEDLINE file to csv")

    date_update_str = date_update.strftime("%Y_%m_%d")
    keys = b.list()
    total_cores = int(sc._conf.get('spark.executor.instances')) * int(sc._conf.get('spark.executor.cores'))
    path_rdd = sc.parallelize(keys, numSlices=3*total_cores)
    parse_results_rdd = path_rdd.flatMap(parse_results_map)
    medline_df = parse_results_rdd.toDF()
    medline_df.write.csv('medline_raw_%s.csv' % date_update_str,
                             mode='overwrite')

    window = Window.partitionBy(['pmid']).orderBy(desc('file_name'))
    windowed_df = medline_df.select(
        max('delete').over(window).alias('is_deleted'),
        rank().over(window).alias('pos'),
        '*')
    windowed_df.\
        where('is_deleted = False and pos = 1').\
        write.csv('medline_lastview_%s.csv' % date_update_str,
                      mode='overwrite')

    # parse grant database
    parse_grant_rdd = path_rdd.flatMap(parse_grant_map)\
        .filter(lambda x: x is not None)\
        .map(lambda x: Row(**x))
    grant_df = parse_grant_rdd.toDF()
    grant_df.write.csv('medline_grant_%s.csv' % date_update_str,
                           mode='overwrite')

conf = SparkConf().setAppName('medline_spark')\
    .setMaster('local[4]')\
    .set('executor.memory', '8g')\
    .set('driver.memory', '8g')\
    .set('spark.driver.maxResultSize', '0')

if __name__ == '__main__':
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)
    is_update, date_update = update()
    if is_update or not glob(os.path.join(save_dir, 'medline_*.csv')):
        process_file(date_update)
    sc.stop()
