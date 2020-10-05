import pytest
from tweet_matcher.matcher import (
    TweetMatcher,
    CategoriesDontMatchError
)
from os import listdir
from shutil import rmtree


class TestMatcher:

    def test_nodes_and_terms_are_valid(self):
        base = './sample/'
        ds = listdir(base)
        check = False
        for node_dir in [base+n for n in ds if 'node' in n]:
            c = str(node_dir).split('_')[-1]
            for terms_dir in [base+t for t in ds if f'terms_{c}' in t]:
                tm = TweetMatcher(node_dir+"/", terms_dir+"/", './sample/tweets/tweets.jsonl.gz', './temp/output')
                tm.get_nodes_and_terms()
                assert len(tm.nodes) == int(c) == len(tm.terms)
                check = True
        assert check

    def test_nodes_and_terms_are_invalid(self):
        tm = TweetMatcher(
            "./sample/nodes_1/",
            "./sample/terms_2_3/",
            './sample/tweets/tweets.jsonl.gz',
            './temp/output'
        )
        with pytest.raises(CategoriesDontMatchError):
            tm.get_nodes_and_terms()
        tm.nodes_dir = "./sample/nodes_2/"
        tm.terms_dir = "./sample/terms_1_3/"
        with pytest.raises(CategoriesDontMatchError):
            tm.get_nodes_and_terms()

    def test_matching_n_grams_from_tweet_in_memory(self):
        tm = TweetMatcher(
            "./sample/nodes_2/",
            "./sample/terms_2_3/",
            './sample/tweets/tweets.jsonl.gz',
            './temp/output/'
        )
        tm.get_nodes_and_terms()
        tm.read_to_memory()
        assert tm.category_count == 2
        assert len(tm.tweets.keys()) == 2
        count_in_memory = 0
        for user_pool in tm.tweets.values():
            assert len(user_pool.keys()) > 0, f"Group {i} has no tweets"
            for user_id, messages_pool in user_pool.items():
                assert len(messages_pool) > 0
                try:
                    for message_id, text in messages_pool:
                        pp_tweet = tm.pre_process_tweet(text)
                        if len(pp_tweet) < 2: continue
                        n_grams_in_tweet = tm.get_n_grams_in_text(pp_tweet)
                        assert len(n_grams_in_tweet) == tm.max_gram_length, f"message {text}"
                        tm.get_match(pp_tweet, user_id)
                        count_in_memory += 1
                except ValueError:
                    assert not messages_pool, f"Message {messages_pool}"

        assert count_in_memory == 7709, f"Total from read to memory {count}"
        total_matches_from_memory_read = len(tm.matches)
        try:
            rmtree(tm.output_dir)
        except FileNotFoundError:
            pass
        assert len(listdir(tm.temp_dir)) == 0

    def test_process_from_gz(self):
        tm = TweetMatcher(
            "./sample/nodes_2/",
            "./sample/terms_2_3/",
            './sample/tweets/tweets.jsonl.gz',
            './temp/output/'
        )
        tm.get_nodes_and_terms()
        tm.run()
        total_matches_reading_from_gz = len(tm.matches)
        assert total_matches_reading_from_gz == 125
        try:
            rmtree(tm.output_dir)
        except FileNotFoundError:
            pass
        assert len(listdir(tm.temp_dir)) == 0

    def test_process_concurrent(self):
        tm = TweetMatcher(
            "./sample/nodes_2/",
            "./sample/terms_2_3/",
            './sample/tweets/tweets_x_10_r_10.jsonl.gz',
            './temp/output/',
            is_concurrent=True
        )
        tm.get_nodes_and_terms()
        tm.run()
        total_matches_concurrent = len(tm.matches)
        assert tm.jsonl_files, "Jsonline list should not be empty"
        assert total_matches_concurrent == 3647
        try:
            rmtree(tm.output_dir)
        except FileNotFoundError:
            pass
        assert len(listdir(tm.temp_dir)) == 0





