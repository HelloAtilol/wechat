# -*- coding: utf-8 -*-
"""

"""

import re


def main():

    fi = open('data/split_result.txt', 'a', encoding='utf-8')

    with open('data/contents.txt', 'r') as f:
        for line in f.readlines():
            if line[0] is ' ':
                line = line[1:]
                print(line)
            if line is '\n':
                continue
            fi.write(line)
    fi.close()


def re_many():
    only_con = "病友之家大白????病友?1234，孩子四化后吐的很历害，还不想吃。怎么办"
    split_at = re.findall(r'@.*?[，\? ]', only_con, re.M)
    print(split_at)
    # print(split_at[1])
    # print(split_at[2])


if __name__ == '__main__':
    # main()
    re_many()
