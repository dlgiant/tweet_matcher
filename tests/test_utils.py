import pytest
from tweet_matcher import utils


class TestUtils:
    def test_get_files_dir_doesnt_exist(self):
        with pytest.raises(FileNotFoundError):
            utils.get_node_ids("./xyz/")
        with pytest.raises(FileNotFoundError):
            utils.get_terms("./xyz/")

    def test_get_files_no_node_files(self):
        with pytest.raises(IsADirectoryError):
            utils.get_node_ids("./sample/")
        with pytest.raises(IsADirectoryError):
            utils.get_terms("./sample/")

    def test_get_ids(self):
        nodes = utils.get_node_ids("./sample/nodes_2/")
        assert len(nodes) == 2
        # tODO: add mock node directory to test n > 2
        for n in nodes:
            assert len(n) >= 2000

    def test_get_terms(self):
        terms, max_l = utils.get_terms("./sample/terms_2_3/")
        assert len(terms) == 2
        # tODO: add mock terms directory to test n > 2 and m> 3
        for t in terms:
            assert len(t) == max_l

        match_count_terms = 0
        for sets in terms:
            for i, t_set in enumerate(sets):
                for g in t_set:
                    match_count_terms += 1
                    assert len(g.split(' ')) - 1 == i, f"Index does not match for {i+1} {g}"

    def test_archive(self):
        pass



