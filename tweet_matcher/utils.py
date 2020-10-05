from functools import wraps, partial
from os import listdir, remove
import logging
from datetime import datetime
import gzip

logging.basicConfig(
    filename="./logs/"+datetime.today().strftime("%Y_%m_%d")+".txt",
    level=logging.NOTSET,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%m-%d-%Y|%I:%M:%S|%p|%Z|'
)
logger = logging.getLogger("UTIL")

def log_process(f):
    @wraps(f)
    def wrapper(*args, **kwds):
        function_name = f.__name__.title()
        filename = f"FILE | {kwds['file']} " if 'file' in kwds else ''
        lines_n = f"LINES | {len(kwds['lines'])} " if 'lines' in kwds else ''
        message = f"START | {function_name} | {filename} | {lines_n} "
        logger.info(f"{message}")
        func = f(*args, **kwds)
        logger.info(f"END | {function_name} ")
        return func
    return wrapper

@log_process
def get_node_ids(nodes_dir):
    """
    Gets node ids and returns 2d list
    1d => category
    2d => ids
    """
    nodes_files = [nodes_dir+f
                   for f in sorted(listdir(nodes_dir))
                   if "nodes" in f]
    logger.info(f"Found node files: {','.join(nodes_files)}")
    nodes = []
    for i, file in enumerate(nodes_files):
        nodes.append(set())
        with open(file, 'r') as f:
            for line in f:
                try:
                    nodes[i].add(int(line.strip()))
                except ValueError as e:
                    logger.info(e)
    for i, sets in enumerate(nodes):
        logger.info(f"Total node ids in set {i+1}: {len(sets)}")
    return nodes

@log_process
def get_terms(terms_dir):
    """
        Gets list of list of sets for terms files

        |term files| = n
        max length gram in given file = m
        sets_terms = list(list(set))
        sets_terms[0][0] <=> set with all 1-gram words in file 1
        sets_terms[0][1] <=> set with all 2-grams in file 1
        ...
        sets_terms[0][m-1] <=> set with all m-grams in file 1
        ...
        sets_terms[n-1][m-1] <=> set with  all m-grams in files n
    """
    terms_files = [terms_dir+f
                   for f in sorted(listdir(terms_dir))
                   if "terms" in f]
    logger.info(f"Found terms files: {','.join(terms_files)}")
    terms = [[] for _ in terms_files]
    max_gram_length = 0
    count_terms = 0
    for i, file in enumerate(terms_files):
        grams = []
        with open(file, 'r') as f:
            for line in f:
                gram = line.strip()
                grams.append(gram)
                count_terms += 1
                max_gram_length = max(max_gram_length, len(gram.split(' ')))
        terms[i] = [set() for _ in range(max_gram_length)]
        for gram in grams:
            terms[i][len(gram.split(' '))-1].add(gram)
    for i, lists in enumerate(terms):
        for j, sets in enumerate(lists):
            logger.info(f"Found {len(sets)} {j+1}-gram(s) in file {i+1}")
    return terms, max_gram_length

@log_process
def compress_jsonl(partition_file, lines):
    with gzip.open(partition_file, mode='wb+') as w:
        logger.info(f"Writing partition {partition_file}")
        for i, l in enumerate(lines):
            w.write(l)

def get_temp_jsonl_files(temp_dir):
    """ Gets temporary jsonl created from partitioning """
    temp_files = [temp_dir+f for f in listdir(temp_dir) if "tweets" in f]
    if not temp_files:
        logger.warning("No json files were created")
    return temp_files

def _max_threads():
    """ Returns the number of available threads on a posix/win based system """
    threads = int(environ['NUMBER_OF_PROCESSORS']) if platform == 'win32' else int(popen('grep -c cores /proc/cpuinfo').read())
    return max(int(threads/2), 1)

@log_process
def archive(file=None, file_dir=None):
    if file:
        split = file.split('/')
        rename(file,f"{ARCHIVE_DIR}/{split[-2]}/{split[-1]}")
    elif file_dir:
        for file in listdir(file_dir):
            archive(file=file)
