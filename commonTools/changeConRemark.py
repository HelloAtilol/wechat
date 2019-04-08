# -*- coding: utf-8 -*-
"""
修改备注的工具类
"""
from commonTools import ConnectDatabase as cd


def main():
    sender_conn = cd.MySQLCommand
    sender_conn.connectMysql(table="wechat_word")
    while True:
        title_list = ["atList"]
        sender_cursor = sender_conn.select_order(title_list=title_list)


if __name__ == '__main__':
    main()
