# -*- coding: utf-8 -*-
"""

"""

import re
from commonTools import wechatContent, ConnectDatabase as cd
import time
import os


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


def wechatClass():
    wechat = wechatContent.WechatContent("wxid_hwg6xtytli7l22:\n@病友之家大白???病友?1234，孩子四化后吐的很历害，还不想吃。怎么办")
    result = wechat.splitAt()
    if len(result) is 0:
        print("没有")
    else:
        print(wechat.splitContent())


def wechat0327():

    message_conn = cd.MySQLCommand()
    message_conn.connectMysql(table="wechat_message")

    contact_conn = cd.MySQLCommand()
    contact_conn.connectMysql(table="wechat_contact")

    chatroomNums = message_conn.select_distinct()
    with open("firstTime.txt", "a", encoding="utf-8") as f:

        for chatroomNum in chatroomNums:
            title_list = ["createTime"]
            situation = "WHERE talker = '%s'" % chatroomNum
            cursor = message_conn.select_order(title_list, situation, order_title="createTime")
            informationTime = cursor.fetchone()[0]
            firstTime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(informationTime) / 1000))
            chatroomName = contact_conn.select_order(["nickname"], "WHERE username = '%s'" % chatroomNum).fetchone()[0]
            f.write(chatroomName + "\t第一次发言:\t" + firstTime + "\n")
    contact_conn.closeMysql()
    message_conn.closeMysql()


def getAbsMenu():
    print(os.getcwd())
    print(os.path.abspath(os.path.dirname(os.getcwd())))


if __name__ == '__main__':
    # main()
    # re_many()
    # wechatClass()
    # wechat0327()
    getAbsMenu()
