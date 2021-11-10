import json
from typing import List


def reset_output_file(filepath):
    with open(filepath, "w", encoding="utf-8") as f:
        f.write("")


def len_output_file(filepath):
    with open(filepath, "r", encoding="utf-8") as f:
        file = f.read()
    return len(file)


def read_output_file(filepath, json_lines: bool = True) -> List:
    if json_lines:
        with open(filepath, "r", encoding="utf-8") as f:
            file = f.readlines()

        lines_json = []
        for line in file:
            lines_json.append(json.loads(line))
        return lines_json

    else:
        with open(filepath, "r", encoding="utf-8") as f:
            file = f.read()
        return file
