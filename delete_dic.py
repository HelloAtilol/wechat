# -*- coding: utf-8 -*-
"""

"""


def main():

    fi = open('data/split_result.txt', 'a', encoding='utf-8')

    with open('data/result.txt', 'r') as f:
        for line in f.readlines():
            if line[0] is ' ':
                line = line[1:]
                print(line)
            if line is '\n':
                continue
            fi.write(line)
    fi.close()


if __name__ == '__main__':
    main()
