from dagster_meltano.dagster_types import _is_list_of_lists


def test_is_list_of_lists_empty():
    assert _is_list_of_lists([])


def test_is_list_of_lists_single_empty():
    assert _is_list_of_lists([[]])


def test_is_list_of_lists_single():
    assert _is_list_of_lists([["foo", "bar"]])


def test_is_list_of_lists_multiple():
    assert _is_list_of_lists([["foo", "bar"], ["spam", "is", "good"]])


def test_is_list_of_lists_error_type():
    assert not _is_list_of_lists({})


def test_is_list_of_lists_error_single_type():
    assert not _is_list_of_lists([{}])


def test_is_list_of_lists_single_error():
    assert not _is_list_of_lists([["spam", 3]])
