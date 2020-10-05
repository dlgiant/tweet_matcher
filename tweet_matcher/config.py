config = {
    'DEBUG':  False,
    'IS_CONCURRENT': False,
    'PARTITION_BY': int(1e9), # 1M tweets
    'MAX_SIZE': int(1e6),   # 1MB
    'TEMP_DIR': "./temp/",
    'ARCHIVE_DIR': "../archive/",
    'LOG_FILE': f"../logs/log_",
}
