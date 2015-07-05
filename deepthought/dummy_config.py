"""This is the config file containing values that can be changed to alter the execution of the program

Attributes:
    ck (str): Consumer Key (API Key) for Twitter's API
    cs (str): Consumer Secret (API Secret) for Twitter's API
    os (str): Access Token for Twitter's API
    ots (str): Access Token Secret for Twitter's API
    boto_access (str): Amazon S3 access key
    boto_secret (str): Amazon S3 secret key
    log_dir (str): Directory where log files are stored
    working_dir (str): Directory where tmp files are stored
    ema_length (int): Length of EMA sample size in seconds
    growth_length (int): Refer to Analyser documentations (in seconds)
    spike_threshold (int): A arbitrary threshold for growth. If the growth is above this threshold, it is considered a spike.
    spike_contents_sample_size (int): An arbitrary value that 'defines' the duration of a spike
    api_port (int): The port for the API server to run on
    api_base_url (str): The url for the API server
"""

# ------- Twitter's API credentials  ------- #
ck = ""
cs = ""
ot = ""
ots = ""

# ------- Amazon S3 credentials ------- #
boto_access = ""
boto_secret = ""

# ------- Directories  ------- #
log_dir = "logs/"
working_dir = "thinking"

# ------- Spike detection settings ------- #
ema_length = 15
growth_length = 10
spike_threshold = 1.3
spike_contents_sample_size = 60 * 5

# ------- API webservice settings  ------- #
api_port = 8000
api_base_url = "/api/"