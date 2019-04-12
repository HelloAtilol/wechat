# -*- coding: utf-8 -*-
"""
修改备注的工具类
"""
from commonTools import ConnectDatabase as cd
from commonTools import wechatContent as wc
import threading

# 声明全局变量
message_conn = cd.MySQLCommand()
message_conn.connectMysql(table="wechat_message")
contact_conn = cd.MySQLCommand()
contact_conn.connectMysql(table="wechat_contact")
update_conn = cd.MySQLCommand()
update_conn.connectMysql(table="wechat_word")


def getInformationBymsgId(word_conn, msgId, nicknames):
    """
    根据msgId获取消息的相关信息
    :param word_conn: 数据库连接
    :param msgId: 信息ID
    :param nicknames: 被@的用户名昵称
    :return:
    """
    title_list = ["content"]
    situation = "WHERE msgId = '%s'" % msgId
    message_conn.select_order(title_list=title_list, situation=situation)
    content = message_conn.cursor.fetchone()[0]
    wechat_content = wc.WechatContent(content)
    result = wechat_content.splitContent()
    userIds = []
    for nickname in nicknames:
        if nickname is "2":
            userIds.append("")
            continue
        name_cursor = contact_conn.select_order(title_list=["username"], situation="WHERE nickname = '%s'" % nickname)
        userId = name_cursor.fetchone()
        if userId is None:
            name_cursor = contact_conn.select_order(title_list=["username"],
                                                    situation="WHERE username = '%s'" % nickname)
            userId = name_cursor.fetchone()
            if userId is None:
                userIds.append("")
            else:
                userIds.append(nickname)
        else:

            userIds.append(userId[0])
    res = {"userId": result["user_id"], "atUserId": "/".join(userIds), "tag": "1"}
    situation = "WHERE msgId = '%s'" % msgId
    word_conn.update_database(res, situation=situation)


def saveResult(chatroomId, chatroomName):
    # 建立数据库连接
    word_conn = cd.MySQLCommand()
    word_conn.connectMysql(table="wechat_word")

    # 构建跨表查询语句，为了分群标注，减少来回查找用户的数量
    sql = "SELECT msgId, context, atList From wechat_word  where tag = 0 and msgId in (SELECT msgId from  wechat_message where talker = '%s')" % chatroomId
    word_cursor = word_conn.cursor
    res = word_cursor.execute(sql)
    print("结果：", res)

    while True:
        res = word_cursor.fetchone()
        # 遍历结束之后，退出循环

        if res is None:
            break
        # 如果没有@的人，进入下一条
        msgId = res[0]
        atList = res[2]
        nicknames = []
        print("第%s条信息正在处理" % msgId)
        # 如果@的人为空，开启新线程修改数据
        # TODO 需要重构，加入对于昵称已经修改好的剔除
        if atList is not "" and atList is not None:
            name_list = atList.split("/")
            for atName in name_list:
                # 设置死循环，进行输入处理
                while True:
                    print("%s\t被@的用户名：%s" % (chatroomName, atName))
                    nickname = input("微信昵称或ID是(输入0代表没有找到，但是@的内容可能存在缺失；输入1代表采用上下文方式补全；输入2代表群内找不到用户)：")
                    print("-----------------" * 5)
                    # 如果找不到用户，可能是解析出来的用户备注名有误，可手动调整
                    if nickname is "0":
                        print("信息原文为：%s" % (res[1]))
                        part_name = input("正确的@内容需要添加(不需要更改输入0)：")
                        # 更改了@信息之后，更新数据库中context内容
                        if part_name is not "0":
                            update_data = {"context": res[1].replace(part_name, part_name + "?")}
                            situation = "WHERE msgId = '%s'" % msgId
                            update_conn.update_database(update_data, situation=situation)
                            atName = atName + part_name
                    elif nickname is "1":
                        next_sql = "SELECT content From wechat_message  where talker = '%s' and msgId >'%s'" % (chatroomId, str(int(msgId)-3))
                        message_conn.cursor.execute(next_sql)
                        print("之前第二条信息：\n %s" % message_conn.cursor.fetchone()[0])
                        print("之前第一条信息：\n %s" % message_conn.cursor.fetchone()[0])
                        print("原信息：\n %s" % message_conn.cursor.fetchone()[0])
                        print("之后第一条信息：\n %s" % message_conn.cursor.fetchone()[0])
                        print("之后第二条信息：\n %s" % message_conn.cursor.fetchone()[0])
                        print("之后第三条信息：\n %s" % message_conn.cursor.fetchone()[0])
                    else:
                        nicknames.append(nickname)
                        break
        # 将数据存储到数据库
        getInformationBymsgId(update_conn, msgId, nicknames)
    # 关闭数据库连接
    word_conn.closeMysql()


def main():
    # 获取所有群编号和群名称
    chatrooms = message_conn.select_distinct()

    for chatroom in chatrooms:
        chatroom = chatroom[0]
        #  查询群名和用户id
        name_sql = "SELECT nickname From wechat_contact WHERE username = '%s'" % chatroom
        message_conn.cursor.execute(name_sql)
        chatroomName = message_conn.cursor.fetchone()[0]
        print("%s 群的@备注修改" % chatroomName)
        print("-----------------" * 3)
        # 处理并保存数据
        saveResult(chatroom, chatroomName)
        # break
    message_conn.closeMysql()
    contact_conn.closeMysql()


if __name__ == '__main__':
    main()
