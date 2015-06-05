# ------- Credentials for Twitter API ------- #
ck = ""
cs = ""
ot = ""
ots = ""

# ------- Path to folder where logs are stored ------- #
log_dir = "logs/"

# ------- Credentials for Amazon S3 ------- #
boto_access = ""
boto_secret = ""

# ------- Vars used when detecting spikes ------- #
# Duration used to calculate EMA and growth
ema_length = 30
growth_length = 10
# Growth threshold for it to be considered a spike
spike_threshold = 1.5
# Length of sample to be used when determining spike's contents
spike_contents_sample_size = 60 * 5

# ------- Host and port for udp sockets ------- #
udp_host = "localhost"
udp_port = 8888