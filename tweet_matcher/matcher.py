"""
    v1.0.0

    Author: Rick Nunes
    Email: sanunes.ricardo@gmail.com

    Tag: Quick and dirty

    Features:
        1. Parsing tweets
        2. Generalizing for up to 9 groups of files

    Assumptions:
        1. node_id's are exclusive to each node_file
        2. Words are exclusive to each terms file
        3. No duplicate tweets

"""
from collections import defaultdict
from datetime import datetime
import argparse
import jsonlines as jsonl
import gzip
from functools import wraps, partial
import multiprocessing as mp
from os import (
    stat,
    environ,
    popen,
    path,
    makedirs,
    listdir,
    remove
)
import threading
from sys import exit
from .utils import (
    get_node_ids,
    get_terms,
    compress_jsonl,
    get_temp_jsonl_files,
    archive,
    logging,
    log_process,
)

from .config import config

logger = logging.getLogger('MAIN')

class CategoriesDontMatchError(Exception):
    """ Raised when node and terms categories don't match """
    pass

class TweetMatcher(object):
    """
        Matches node ids in different cathegories to terms in equivalent
        cathegories, parsing tweets and matching n-grams found in terms
    """
    temp_dir = config['TEMP_DIR']
    matches = set()
    tweets = dict()

    def __init__(
        self,
        nodes_dir,
        terms_dir,
        tweets_file,
        output_dir,
        partition_to_jsonl_by=config['PARTITION_BY'],
        is_concurrent=config['IS_CONCURRENT'],
    ):
        self.nodes_dir = nodes_dir
        self.terms_dir= terms_dir
        self.is_concurrent = is_concurrent
        self.partition_to_jsonl_by = partition_to_jsonl_by
        self.tweets_file = tweets_file
        self.output_dir = output_dir

    def get_nodes_and_terms(self):
        self.nodes = get_node_ids(self.nodes_dir)
        self.terms, self.max_gram_length = get_terms(self.terms_dir)
        if len(self.nodes) != len(self.terms):
            raise CategoriesDontMatchError
        self.category_count = len(self.nodes)
        for c in range(self.category_count):
            #tweets_in_category = dict()
            self.tweets[c] = dict()

    @log_process
    def run(self):
        if not self.is_concurrent or stat(self.tweets_file).st_size < config['MAX_SIZE']:
            self.process()
        else:
            self.process_concurrent()
        self.write_matches()

    @log_process
    def process_concurrent(self):
        """
            Handles compressed tweets file in parallel after partitioning
        """
        logger.info(f"Processing {self.tweets_file}")
        lines = []
        total_partitions = 0
        partitioner = self.partition_to_jsonl_by
        with gzip.open(self.tweets_file, 'rb') as f:
            for index, line in enumerate(f):
                lines.append(line)
                if index > partitioner:
                    total_partitions += 1
                    partition_file = f"{self.temp_dir}/tweets_up_to_{partitioner}.jsonl.gz"
                    compress_jsonl(partition_file, lines)
                    partitioner += self.partition_to_jsonl_by
                    lines = []

        if lines:
            total_partitions += 1
            partitioner = partitioner - (self.partition_to_jsonl_by + len(lines)) if total_partitions > 1 else len(lines)
            last_partition_file = f"{self.temp_dir}/tweets_up_to_{partitioner}.jsonl.gz"
            compress_jsonl(last_partition_file, lines)

        logger.info(f"Partitions created: {total_partitions}")
        self.jsonl_files = get_temp_jsonl_files(self.temp_dir)
        self.get_matches_concurrent()
        self.clean_up()

    @log_process
    def process(self):
        """
            Reads compressed file sequentially and gets matches on the go
        """
        logger.info(f"Processing {self.tweets_file}")
        count_lines = 0
        count_filtered = 0
        with gzip.open(self.tweets_file, 'rb') as f:
            reader = jsonl.Reader(f)
            for i, tweet in enumerate(reader.iter(type=dict, skip_invalid=True)):
                count_lines += 1
                pp_tweet = self.pre_process_tweet(tweet['text'])
                if len(pp_tweet) < 2: continue
                count_filtered += 1
                self.get_match(pp_tweet, tweet['node_id'])
        logger.info(f"# TWEETS | PROCESSED {count_lines} | FILTERED {count_filtered} ")

    @log_process
    def read_to_memory(self):
        with gzip.open(self.tweets_file, 'rb') as f:
            reader = jsonl.Reader(f)
            for tweet in reader.iter(type=dict, skip_invalid=True):
                for i, node_set in enumerate(self.nodes):
                    if int(tweet['node_id']) in node_set:
                        pp_tweet = self.pre_process_tweet(tweet['text'])
                        # Ignore empty preprocessed tweets and single characters
                        if len(pp_tweet) < 2 and not tweet['message_id']: continue
                        try:
                            self.tweets[i][tweet['node_id']].add((tweet['message_id'], tweet['text']))
                        except KeyError:
                            self.tweets[i][tweet['node_id']] = set([(tweet['message_id'], tweet['text']),])

    @staticmethod
    def pre_process_tweet(tweet_text):
        """
            Removes some special characters, makes lower case and
            ignores embedded links
        """
        return [word.lower().strip().replace("‘", "").replace("’", "")\
                .replace(",","").replace(":","").replace("!","")\
                .replace("#","").replace("...","").replace('"','')
                for word in tweet_text.split(' ')
                if "http" not in word and len(word) > 1]

    def get_n_grams_in_text(self, pp_tweet):
        """
            Iterates over each word in tweet text and
            generates sets of n-grams starting with each
        """
        n_grams_in_text = defaultdict(set)
        for a in range(len(pp_tweet)):
            """ Creating n-gram sets """
            for step in range(1, self.max_gram_length+1):
                n_gram = " ".join(pp_tweet[a:a+step])
                try:
                    n_grams_in_text[step-1].add(n_gram)
                except IndexError as e:
                    logger.warning(e, step, self.max_gram_length)
        return n_grams_in_text

    def match_text_n_grams_with_terms(
        self,
        node_id,
        node_c,
        n_grams_in_text,
        matches_set
    ):
        """
            Matches generated n-grams from tweet text with stored terms
            in given category
        """
        for n, n_grams in n_grams_in_text.items():
            for gram in n_grams:
                try:
                    if gram in self.terms[node_c][n]:
                        matches_set.add((node_id, gram))
                except IndexError as e:
                    logger.warning(f"{e} in terms {i}, {j}")

    def get_match(self, pp_tweet, node_id, matches_set=None):
        """
            Processes match considering node id category
        """
        matches_set = self.matches if not matches_set else matches_set
        for node_category, id_set in enumerate(self.nodes):
            if int(node_id) in id_set:
                n_grams_in_text = self.get_n_grams_in_text(pp_tweet)
                self.match_text_n_grams_with_terms(
                    node_id,
                    node_category,
                    n_grams_in_text,
                    matches_set
                )

    @log_process
    def get_matches_concurrent(self):
        for file in self.jsonl_files:
            t = threading.Thread(target=self.get_matches_in_jsonl, args=(file, self.matches))
            t.start()

        main_thread = threading.currentThread()
        for t in threading.enumerate():
            if t is not main_thread:
                t.join()

    def get_matches_in_jsonl(self, file, matches_set):
        logger.info(f"Started: {file}")
        with gzip.open(file) as f:
            reader = jsonl.Reader(f)
            for tweet in reader.iter(type=dict, skip_invalid=True):
                pp_tweet = self.pre_process_tweet(tweet['text'])
                if len(pp_tweet) < 2: continue
                self.get_match(pp_tweet, tweet['node_id'],  matches_set)
        logger.info(f"Finished: {file}")

    @log_process
    def write_matches(self):
        if len(self.matches):
            output_file = self.output_dir + "result_"+datetime.today().strftime("%Y_%m_%d")+".txt"
            if not path.exists(self.output_dir): makedirs(self.output_dir[:-1])
            with open(output_file, 'w+') as w:
                for (node_id, term) in self.matches:
                    w.write(f"{node_id} {term}\n")
        else:
            logger.warning("No matches found")

    @log_process
    def clean_up(self):
        for file in self.jsonl_files:
            remove(file)
        logger.info("Removed temporary jsonl")
        logger.info("Job completed successfully")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-p",
        help="Partition gz file by p tweets, default is 100000",
        type=int,
        default=100000
    )
    parser.add_argument(
        "-rpt",
        help="Read from partition",
        type=bool,
        default=False
    )
    parser.add_argument("-pgz", help="Parse from gz", type=bool, default=True)
    parser.add_argument("-nd", help="Path to nodes directory")
    parser.add_argument("-td", help="Path to terms directory")
    parser.add_argument("-tw", help="Path to tweets file")
    parser.add_argument("-od", help="Path to output directory")
    parser.add_argument("-ard", help="Path to archive")
    parser.add_argument("-cc", help="Make matching concurrent", default=False)
    args = parser.parse_args()

    tm = TweetMatcher(
        nodes_dir=args.nd,
        terms_dir=args.td,
        tweets_file=args.tw,
        output_dir=args.od,
        partition_to_jsonl_by=args.p,
        is_concurrent=args.cc
    )
    try:
        tm.get_nodes_and_terms()
    except (
        IsADirectoryError,
        FileNotFoundError,
        CategoriesDontMatchError
    ) as e:
        # TODO: Implement email alert
        logger.warning(e)
        logger.warning("Exiting program,  directories are not properly set")
        exit()
    tm.run()

