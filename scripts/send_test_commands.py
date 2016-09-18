from argparse import ArgumentParser
from sys import path
from os.path import dirname as dirn

path.append(dirn(path[0]))

from config.praw_config import get_test_command_site_names, get_site_names_from_bot_name, PRAW_FILE_PATH
import praw
import bots
import yaml
from configparser import ConfigParser

import os
test_command_path = "test_commands.yaml"


def get_logged_in_reddit(user_name, message_recipient):
    user_agent_template = "/u/{} sending PMs to /u/{} to test TicketBot functionality for the FAUbot project. " \
                          "See github.com/palmerjoshua/FAUbot.git".format(user_name, message_recipient)
    r = praw.Reddit(user_agent=user_agent_template, site_name=user_name)
    try:
        current_access_info = r.refresh_access_information()
    except praw.errors.HTTPException:
        raise bots.MissingRefreshTokenError("No oauth refresh token saved. Please run account_register.py.")
    r.set_access_credentials(**current_access_info)
    return r


def send_test_message(sender, recipient, subject, message):
    r = get_logged_in_reddit(sender, recipient)
    r.send_message(recipient, subject, message)


def generate_test_message_create():
    message_template = "!FAUbot {} {} {}"
    operation = input("Enter operation (buy/sell): ").lower()
    amount = int(input("Enter amount (1-99): "))
    date = input("Enter ceremony date: ")
    return message_template.format(operation, amount, date)


def generate_test_message_delete():
    return "!FAUbot delete me"


def get_test_commands():
    dir_name = os.path.dirname(os.path.realpath(__file__))
    command_file_name = os.path.join(dir_name, test_command_path)
    with open(command_file_name, 'r') as  infile:
        config = yaml.load(infile)
    return {user_name: config[user_name] for user_name in config}


def work(senders, recipient):
    subject = "Test Message"
    test_commands = get_test_commands()
    for sender in senders:
        if sender in test_commands:
            commands = test_commands[sender]
            for command in commands:
                send_test_message(sender, recipient, subject, command)
        else:
            print("Sender", sender, "not in test_commands.yaml. No command will be sent to TicketBot from that user.")


def main():
    praw_config_parser = ConfigParser()
    praw_config_parser.read(PRAW_FILE_PATH)
    cli_argument_parser = ArgumentParser(description="Send test commands to TicketBot")
    cli_argument_parser.add_argument('-r', '--recipient', choices=get_site_names_from_bot_name('TicketBot', praw_config_parser), dest='recipient', required=True)
    cli_argument_parser.add_argument('-s', '--senders', nargs='+', choices=get_test_command_site_names(praw_config_parser), dest='senders', required=True)

    args = cli_argument_parser.parse_args()
    work(args.senders, args.recipient)

if __name__ == '__main__':
    main()
