# -*- coding: utf-8 -*-
"""
读取文件，只读取内容部分，并做简单分析。
author: 王诚坤
date: 2018/10/30
update: 2018/11/2
"""

from ConnectDatabase import MySQLCommand
import re
import jieba
from wordcloud import WordCloud
from matplotlib import pyplot as plt

# 用户分别被@的次数
at_dict = {}


def order_content(conn):
    """
    按照时间先后顺序获得纯文本内容
    :param conn:
    :return:
    """
    last_sid = ''
    title_list = ['createTime', 'msgSvrId', 'content']
    result = conn.select_order(title_list)
    f = open('data/content.txt', 'w', encoding='utf-8')
    for res in result:
        # 去除重复
        if res[1] != last_sid:
            content = res[2]
            only_content = get_id_content(content)
            f.write(only_content + '\n')
            last_sid = res[1]
    f.close()
    return at_dict


def get_id_content(content):
    re_con = re.compile(r'^(.*)(:\n)(.*)')
    re_at = re.compile(r'^(.*?)(@)(.*?)([\?\s]+)')
    id_content = re_con.match(content)
    global at_dict
    try:
        only_content = content.replace(id_content.group(1) + id_content.group(2), '')
        if '<' in only_content:
            return only_content
        at_content = re_at.match(only_content)
        if at_content is not None:
            only_at = at_content.group(3)
            only_content = only_content.replace(at_content.group(0), '')
            if only_at not in at_dict:
                at_dict[only_at] = 1
            else:
                at_dict[only_at] += 1
            print(only_at)
    except AttributeError:
        only_content = ''
    return only_content


def get_stopwords(filepath):
    stopwords = [line.strip() for line in open(filepath, 'r', encoding='utf-8').readlines()]
    return stopwords


def split_word():
    print('******开始分词！******')
    # 加载停用词
    stopwords = get_stopwords('data/stopwords.txt')
    f = open('data/content.txt', encoding='utf-8')
    result = ''
    for line in f.readlines():
        seg = jieba.cut(line)
        for word in seg:
            if word not in stopwords:
                result = result + word + ' '
    f.close()
    wc = WordCloud(background_color='white', width=1000, height=800, font_path='data/msyh.ttc')
    wc.generate_from_text(result)
    plt.imshow(wc)
    plt.axis('off')
    plt.figure()
    plt.show()
    # print(contents)
    print('******分词结束！******')


def main():
    # conn = MySQLCommand()
    # conn.connectMysql()
    # order_content(conn)
    # conn.closeMysql()
    # print(at_dict)
    split_word()


if __name__ == '__main__':
    main()
