import re
from config import getLogger
from bots import RedditBot
from dateutil.parser import parse
from collections import OrderedDict, namedtuple
from cachetools import ttl_cache, TTLCache
import requests
from bs4 import BeautifulSoup
import boto3, boto3.dynamodb
logger = getLogger()

CEREMONY_URL = "https://www.fau.edu/registrar/graduation/ceremony.php"

CommandTuple = namedtuple('CommandTuple', 'user operation number date')
ResolveTuple = namedtuple('ResolveTuple', 'user resolved_with')


class TicketBot(RedditBot):
    def __init__(self, user_name, *args, **kwargs):
        super().__init__(user_name, *args, reset_sleep_interval=False, **kwargs)
        self.COMMAND_PATTERN = "^!FAUbot (buy|sell) (\d{1,2}) (?:(?:[A-Za-z,]+ ?)?([A-Za-z]+ \d{1,2}(?:th)? (?:at )?\d{1,2} ?[AaPp][Mm]))?$"
        self.RESOLVE_PATTERN = "^!FAUbot resolve (\d{1,2}) \/u\/([\w_]+)$"
        self.DELETE_COMMAND = "!FAUbot delete me"
        self._command_regex = re.compile(self.COMMAND_PATTERN)
        self._resolve_regex = re.compile(self.RESOLVE_PATTERN)
        self._ticketbot_table = self._get_ticketbot_table()
        self.sleep_interval = 30
        self.ceremony_dict = self.get_ceremony_dict()

    @staticmethod
    def _get_ceremony_data():
        r = requests.get(CEREMONY_URL)
        html = r.text
        soup = BeautifulSoup(html, 'html.parser')

        table_body = soup.tbody
        strongs = table_body.find_all('strong')
        return [strong.get_text().strip().replace("\n", "") for strong in strongs]

    @staticmethod
    def _get_ceremony_dict(data):
        result = OrderedDict()
        current_date = None
        for d in data:
            try:
                dt = parse(d)
            except ValueError:
                if current_date:
                    result[current_date].add(d)
            else:
                if dt.hour == 0:
                    continue
                if d not in result and not any(key.startswith(d) for key in result):
                    result[d] = set()
                current_date = d
        return result

    def get_ceremony_dict(self):
        data = self._get_ceremony_data()
        return self._get_ceremony_dict(data)

    def parse_command(self, message):
        logger.info("Parsing message: message=[{}]".format(message.body))
        if "!FAUbot" not in message.body:
            raise NoCommandInMessage

        command = self._command_regex.match(message.body)
        if command:
            operation = command.groups()[0]
            number = command.groups()[1]
            try:
                dt = parse(command.groups()[2])
            except (ValueError, KeyError):
                date = None
            else:
                try:
                    date = next((d for d in self.ceremony_dict if parse(d) == dt))
                except StopIteration:
                    raise InvalidCeremonyDate(message.author, operation, number, command.groups()[2], self.ceremony_dict.keys())
            return CommandTuple(user=message.author.name, operation=operation, number=number, date=date)
        else:
            raise InvalidCommand(message.author, message.body)



    def _send_invalid_ceremony_date_message(self, err):
        logger.info("Sending invalid ceremony date message: user=[{}], givenDate=[{}]".format(err.user, err.given_date))
        message = "Uh oh! Your last command included a date on which there are no ceremonies scheduled.\n\n" \
                  "Your last command was: `{} {} {}`\n\n" \
                  "Here are your choices:\n\n".format(err.operation, err.number, err.given_date)
        for date in self.ceremony_dict.keys():
            message += "* {}\n".format(date)
        message += "\n\nYou may try again with one of those dates."
        subject = "Invalid Ceremony Date in Your Command"
        self.r.send_message(err.user, subject, message)

    def _get_ticketbot_table(self):
        db = boto3.resource('dynamodb', region_name='us-east-1')
        return db.Table('ticketbot')

    def _add_new_db_record(self, command_tuple):
        logger.info("Saving new record in database: {}".format(command_tuple))
        self._ticketbot_table.put_item(Item={
            'user_name': command_tuple.user,
            'operation': command_tuple.operation,
            'number': command_tuple.number,
            'date': command_tuple.date,
            'is_resolved': False
        })

    def _set_user_resolved(self, user_name, resolved_with):
        logger.info("Resolving transaction between: user=[{}], resolvedWith=[{}]".format(user_name, resolved_with))
        logger.info("Updating user in database: user=[{}]".format(user_name))
        self._ticketbot_table.update_item(
            Key={'user_name': user_name},
            UpdateExpression='SET resolved = :resolved_val, resolved_with = :resolved_with',
            ExpressionAttributeValues={':resolved_val': True, ':resolved_with': resolved_with}
        )
        logger.info("Updating user in database: user=[{}]".format(resolved_with))
        self._ticketbot_table.update_item(
            Key={'user_name': resolved_with},
            UpdateExpression='SET resolved = :resolved_val, resolved_with = :resolved_with',
            ExpressionAttributeValues={':resolved_val': True, ':resolved_with': user_name}
        )

    def _update_user_ticket_number(self, user_name, new_number):
        logger.info("Updating ticket number in database: user=[{}], newNumber=[{}]".format(user_name, new_number))
        self._ticketbot_table.update_item(
            Key={'user_name': user_name},
            UpdateExpression='SET number = :new_number',
            ExpressionAttributeValues={':new_number': new_number}
        )

    def _delete_user(self, user_name):
        logger.info("Deleting user from database: user=[{}]".format(user_name))
        self._ticketbot_table.delete_item(Key={'user_name': user_name})

    def _send_invalid_command_message(self, err):
        logger.info("Sending invalid command message: user=[{}]".format(err.user))
        message = "Uh oh! You send an invalid command in your message. Here is what you sent me:\n\n"
        for line in err.message_body.split('\n'):
            message += ">{}\n".format(line)
        message += "\n\nTry another command."
        subject = "Invalid Command"
        self.r.send_message(err.user, subject, message)

    def _send_confirmation_message(self, command_tuple):
        logger.info("Sending confirmation message: user=[{}]".format(command_tuple.user))
        message = "Your request has been processed. Your command:\n\n" \
                  "`{} {} {}`\n\n" \
                  "You're in the database now.".format(command_tuple.operation, command_tuple.number, command_tuple.date)
        subject = "Successfully Processed Request"
        self.r.send_message(command_tuple.user, subject, message)

    def work(self):
        logger.info("Getting unread messages")
        inbox = self.r.get_unread(unset_has_mail=True)
        for message in inbox:
            if self.DELETE_COMMAND in message.body:
                self._delete_user(message.author.name)
                message.mark_as_read()
            else:
                try:
                    command = self.parse_command(message)
                except InvalidCeremonyDate as err:
                    self._send_invalid_ceremony_date_message(err)
                    message.mark_as_read()
                except InvalidCommand as err:
                    self._send_invalid_command_message(err)
                    message.mark_as_read()
                except NoCommandInMessage:
                    continue
                else:
                    self._add_new_db_record(command)
                    self._send_confirmation_message(command)
                    message.mark_as_read()


class NoCommandInMessage(Exception):
    pass


class InvalidCommand(ValueError):
    def __init__(self, user, message_body):
        self.user = user
        self.message_body = message_body
        msg = "Invalid TicketBot command. Message:\n\n{}".format(message_body)
        super(InvalidCommand, self).__init__(msg)


class InvalidCeremonyDate(ValueError):
    def __init__(self, user, operation, number, given_date, choices):
        self.user = user
        self.number = number
        self.operation = operation
        self.given_date = given_date
        msg = "Invalid Ceremony Date: given=[{}], choices={}".format(self.given_date, choices)
        super(InvalidCeremonyDate, self).__init__(msg)


def main():
    from config.praw_config import get_all_site_names
    from argparse import ArgumentParser
    parser = ArgumentParser("Running TicketBot by itself")
    parser.add_argument("-a", "--account", dest="reddit_account", required=True, choices=get_all_site_names(),
                        help="Specify which Reddit account entry from praw.ini to use.")
    args = parser.parse_args()
    test = TicketBot(args.reddit_account, run_once=True)
    test.start()
    test.stop_event.wait()


if __name__ == '__main__':
    main()


if __name__ == '__main__':
    bot = TicketBot('FAUbot', run_once=True)
    bot.start()
    bot.stop_event.wait()
