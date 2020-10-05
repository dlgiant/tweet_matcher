import gzip
import jsonlines as jsonl
from random import sample, uniform
import argparse
from itertools import chain
import sys
from tweet_matcher.matcher import TweetMatcher
import json

TWEETS_FILE_GZ = "./sample/tweets/tweets.jsonl.gz"
BENCH_GEN_DIR = "./sample/tweets/"

class CreateRandomTweets(object):
    source = TWEETS_FILE_GZ
    target_dir = BENCH_GEN_DIR

    def __init__(self, nodes, terms):
        self.nodes = set(chain.from_iterable(nodes))
        self.terms = terms

    def multiply_from_original(self, n=100, p=0.10):
        """ Assign random tweet to nodes in files """
        count_tweets = 0
        tweets = []
        target_file = f"{self.target_dir}/tweets_x_{n}_r_{int(p*100)}.jsonl.gz"
        tweets = []
        with gzip.open(self.source, 'rb') as f:
            reader = jsonl.Reader(f)
            not_in_node_sets = set([0])
            for tweet in reader.iter(type=dict, skip_invalid=True):
                tweets.append(tweet)

        n_tweets = []
        for i, tweet in enumerate(tweets):
            if tweet['node_id'] not in self.nodes:
                not_in_node_sets.add(tweet['node_id'])
            for _ in range(n):
                tweet['node_id'] = str(sample(self.nodes, 1)[0]) if uniform(0.0, 1.0) < p else str(sample(not_in_node_sets, 1)[0])
                n_tweets.append(json.dumps(tweet).encode("utf-8")+"\n".encode("utf-8"))

        with gzip.open(target_file, 'wb+') as w:
            for tweet in n_tweets:
                w.write(tweet)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-n",
        help="Multiplication factor, default is 100",
        type=int,
        default=100
    )
    parser.add_argument(
        "-p",
        help="Include percetange of nodes from sets, provide value in range 0 <= r <= 100",
        type=int,
        default=10
    )
    args = parser.parse_args()
    print(f"Generating {args.n} times from tweet sample with {args.p}% node ids included in set")
    tm = TweetMatcher('./sample/nodes_2/', './sample/terms_2_3/', './sample/tweets/tweets.jsonl.gz', './temp/output')
    tm.get_nodes_and_terms()
    rt = CreateRandomTweets(nodes=tm.nodes, terms=tm.terms)
    rt.multiply_from_original(n=args.n, p=float(args.p)/100)



