import pprint
from threading import Thread
from typing import Any
from datetime import datetime
import redis

channels = redis.Redis(host='localhost', port=6379, db=0, charset="utf-8", decode_responses=True)
history = redis.Redis(host='localhost', port=6379, db=1, charset="utf-8", decode_responses=True)
users = redis.Redis(host='localhost', port=6379, db=2, charset="utf-8", decode_responses=True)
ch_pub_sub = channels.pubsub()


# server functions
def login(username1, password1) -> bool:
    return users.exists(username1) == 1 and users.get(username1) == password1


def signup(username1, password1) -> bool:
    if users.exists(username1):
        return False
    else:
        users.set(username1, password1)
        return True


def get_channels() -> dict[Any, Any]:
    channel_names = channels.keys()
    channel_dic = dict()
    for channel in channel_names:
        info = dict({channel: channels.hgetall(channel)})
        channel_dic.update(info)
    return channel_dic


def subscribe(names):
    for name in names:
        ch_pub_sub.subscribe(name)
        # add member to the channel
        members_list = channels.hget(name, "members")
        if username not in members_list:
            members_list = members_list + username + ", "
            channels.hset(name, "members", members_list)


def unsubscribe(name):
    ch_pub_sub.unsubscribe(name)
    # remove user from the channel
    members_list = channels.hget(name, "members")
    members_list = members_list.replace(username + ",", "")
    channels.hset(name, "members", members_list)


def publish():
    while True:
        text = input()
        msg = text.split(" ")
        channel = msg[1]
        keyword = msg[0]
        content = text.replace(msg[0], "").replace(msg[1], "")
        if keyword == "LEAVE":
            unsubscribe(channel)
        elif keyword == "PUB":
            unsubscribe(channel)
            time = datetime.now()
            message = "[" + str(time) + "] " + username + " (" + channel + ") :" + content
            history.hset(channel, str(time), message)  # backup message to history db
            channels.publish(channel, message)
            names = set()
            names.add(channel)
            subscribe(names)
            chatting()
        elif keyword == "0":
            break


def channel_exist(name) -> bool:
    return channels.exists(name) == 1


def username_exists():
    return users.exists(username) == 1


def add_channel(name, desc):
    members = username + ","
    channels.hset(name, "desc", desc)
    channels.hset(name, "creator", username)
    channels.hset(name, "date", str(datetime.now()))
    channels.hset(name, "members", members)
    # don't forget last n hour messages


####################################################################
# client functions

def create_channel():
    while True:
        name = input("channel name: ")
        if " " in name:
            print("channel name cannot include whitespace")
        if name == "0":
            break
        if channel_exist(name):
            print("this name is already taken. press 0 to exit")
        else:
            print("please write a short description for your channel")
            desc = input()
            add_channel(name, desc)
            print("channel created successfully")
            break


def show_channels():
    channels_list = get_channels()
    if not bool(channels_list):
        print("there is no channel available")
    else:
        print("choose the channels you want to subscribe to.\n"
              "enter each channel name in  a new line and type done when finished.\n"
              "press 0 to go back to main menu\n")
        pprint.pprint(channels_list)
        print("\nto publish a message to a channel, type: PUB {channel name} {msg}\n"
              "to stop receiving messages from a channel type: LEAVE {channel name}")
        names = []
        while True:
            name = input()
            if name == "done":
                break
            elif name == "0":
                return
            elif not channel_exist(name):
                print(f"channel {name} doesn't exist")
            else:
                names.append(name)
        subscribe(names)
        chat_history(names, 2)
        chatting()


def chatting():
    thread = Thread(target=publish)
    thread.start()
    while True:
        msg = ch_pub_sub.get_message()
        if msg is not None and not msg["data"] == 1 and not msg["data"] == 0 and not msg["data"] == 2:
            print(msg["data"])


def chat_history(names, hours):
    print("currently at chat history")
    current_time = datetime.now()

    for name in names:

        chat_dict = history.hgetall(name)  # gives all the time-msg pairs as a dictionary
        if not bool(chat_dict):
            print("==============================================================================")
            print(f"there are no messages in channel {name} yet. be the first one to say hello!\n")
        else:
            print("==============================================================================")
            print(F"recent messages in channel {name} :\n")

            for key in sorted(chat_dict.keys()):
                time = datetime.strptime(key, "%Y-%m-%d %H:%M:%S.%f")
                if current_time.hour - time.hour <= hours:
                    print(chat_dict[key] + "\n")


def menu():
    while True:
        print("1)channels list\n"
              "2)create channel")

        choice = input()
        if choice == "1":
            show_channels()
        elif choice == "2":
            create_channel()


if __name__ == '__main__':

    while True:
        print("welcome\n"
              "1)sign in\n"
              "2)login")
        choice = int(input())
        if choice == 1:
            while True:
                username = input("username: ")
                if username == "0":
                    break
                password = input("password: ")
                if signup(username, password):
                    print(f"welcome {username}")
                    menu()
                else:
                    print("this username is already taken. press 0 to exit")

        elif choice == 2:
            while True:
                username = input("username: ")
                if username == "0":
                    break
                password = input("password: ")
                if login(username, password):
                    print(f"welcome {username}")
                    menu()

                else:
                    print("wrong username or password! press 0 to exit")
