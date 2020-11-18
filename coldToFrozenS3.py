# Modified $SPLUNK_HOME/bin/coldToFrozenExample.py
# Splunk Archiving Frozen Data script

import sys, os, gzip, shutil, subprocess

# Get Indexer specific role
file = open("/etc/splunk/backup-role", "r")
backupRole = file.readline()
backupRole = backupRole.strip()

######### Add <S3 URL>
destination = "<S3 URL>" + backupRole + "/"

# For new style buckets (v4.2+), we can remove all files except for the rawdata.
# We can later rebuild all metadata and tsidx files with "splunk rebuild"
def handleNewBucket(base, files):
    print 'Archiving bucket: ' + base
    for f in files:
        full = os.path.join(base, f)
        if os.path.isfile(full):
            os.remove(full)

# For buckets created before 4.2, simply gzip the tsidx files
# To thaw these buckets, be sure to first unzip the tsidx files
def handleOldBucket(base, files):
    print 'Archiving old-style bucket: ' + base
    for f in files:
        full = os.path.join(base, f)
        if os.path.isfile(full) and (f. endswith('.tsidx') or f.endswith('.data')):
            fin = open(full, 'rb')
            fout = gzip.open(full + '.gz', 'wb')
            fout.writelines(fin)
            fout.close()
            fin.close()
            os.remove(full)

if __name__ == "__main__":
    if len(sys.argv) != 2:

        ########## Add bucket directory name
        sys.exit('usage: python coldToFrozen.py <s3_bucket_dir_to_archive>')

    bucket = sys.argv[1]
    if not os.path.isdir(bucket):
        sys.exit('Given bucket is not a valid directory: ' + bucket)

    rawdatadir = os.path.join(bucket, 'rawdata')
    if not os.path.isdir(rawdatadir):
        sys.exit('No rawdata directory, given bucket is likely invalid: ' + bucket)

    files = os.listdir(bucket)
    journal = os.path.join(rawdatadir, 'journal.gz')
    if os.path.isfile(journal):
        handleNewBucket(bucket, files)
    else:
        handleOldBucket(bucket, files)

    if bucket.endswith('/'):
        bucket = bucket[:-1]

    indexname = os.path.basename(os.path.dirname(os.path.dirname(bucket)))
    destination = destination + indexname + "/" + os.path.basename(bucket)

    try:
	      # Can adjust the properties for S3 as necessary
        retval = subprocess.check_output(["aws", "s3", "cp", bucket, destination, "--sse", "--storage-class", "STANDARD_IA", "--recursive"])
    except:
        sys.exit(1)
