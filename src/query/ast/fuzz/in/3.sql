COPY INTO ontime200 from @s1 FILES = ('ontime_200.csv.gz') FILE_FORMAT = (type = CSV field_delimiter = ',' compression = 'gzip'  record_delimiter = '\n' skip_header = 1);