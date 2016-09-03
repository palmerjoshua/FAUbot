import re
from config import getLogger
from bots import RedditBot
from dateutil.parser import parse
from collections import OrderedDict, namedtuple
from cachetools import ttl_cache, TTLCache
from queue import Queue
import requests
from bs4 import BeautifulSoup
import boto3, boto3.dynamodb
from boto3.dynamodb.conditions import Key, Attr
logger = getLogger()

CEREMONY_URL = "https://www.fau.edu/registrar/graduation/ceremony.php"

CommandTuple = namedtuple('CommandTuple', 'user operation amount date')
ResolveTuple = namedtuple('ResolveTuple', 'user resolve_with resolve_amount')


class TicketBot(RedditBot):
    def __init__(self, user_name, *args, **kwargs):
        super().__init__(user_name, *args, reset_sleep_interval=False, **kwargs)
        self.sleep_interval = 30
        self.DELETE_COMMAND = "!FAUbot delete me"
        self.TRIGGER = ""  # Don't use characters that need to be escaped in regular expressions
        self.COMMAND_PATTERN = "^!FAUbot (buy|sell) (\d{1,2})(?: (.+))?$"
        self.RESOLVE_PATTERN = "^!FAUbot resolve \/u\/([\w_-]+)(?: (\d{1,2})?)?$"
        self._command_regex = re.compile(self.COMMAND_PATTERN)
        self._resolve_regex = re.compile(self.RESOLVE_PATTERN)
        self._ticketbot_table = self._get_ticketbot_table()
        self.ceremony_dict = self.get_ceremony_dict()
        self.add_queue = Queue()
        self.delete_queue = Queue()

        pass

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

    def _queue_data_present(self):
        return not self.add_queue.empty() or not self.delete_queue.empty()

    def get_ceremony_dict(self):
        data = self._get_ceremony_data()
        return self._get_ceremony_dict(data)

    def _get_ticketbot_table(self):
        db = boto3.resource('dynamodb', region_name='us-east-1')
        return db.Table('ticketbot')

    def _get_user_item(self, command_tuple):
        """
        :type command_tuple: CommandTuple
        :param command_tuple:
        :return:
        """
        return {
            'user_name': command_tuple.user.lower(),
            'operation': command_tuple.operation,
            'amount': command_tuple.amount,
            'date': command_tuple.date,
            'resolved': False
        }

    def _get_user_key(self, user_name):
        return {'user_name': user_name.lower()}

    def _add_new_db_record(self, command_tuple):
        """
        :type command_tuple: CommandTuple
        :param command_tuple:
        :return:
        """
        logger.info("Saving new record in database: {}".format(command_tuple))
        self._ticketbot_table.put_item(Item=self._get_user_item(command_tuple))

    def _new_user_add_queue(self, command_tuple):
        """
        :type command_tuple: CommandTuple
        :param command_tuple:
        :return:
        """
        self.add_queue.put(self._get_user_item(command_tuple))

    def _new_user_delete_queue(self, user_name):
        self.delete_queue.put(self._get_user_key(user_name))

    def _batch_write_new_db_records(self):
        notify_functions = {}
        if self._queue_data_present():
            with self._ticketbot_table.batch_writer() as batch:
                while not self.delete_queue.empty():
                    item = self.delete_queue.get()
                    notify_functions[item['user_name'].lower()] = self._send_delete_confirmation_message
                    logger.info("Deleting DB record: Key=[{}]".format(item))
                    batch.delete_item(Key=item)
                while not self.add_queue.empty():
                    item = self.add_queue.get()
                    notify_functions[item['user_name'].lower()] = self._send_confirmation_message
                    logger.info("Writing new DB record: Item=[{}]".format(item))
                    batch.put_item(Item=item)
        else:
            logger.info("All Queues are empty. Not writing anything to database.")
        return notify_functions

    def _delete_user(self, user_name):
        logger.info("Deleting user from database: user=[{}]".format(user_name))
        self._ticketbot_table.delete_item(Key=self._get_user_key(user_name))

    def _set_user_resolved(self, resolve_tuple):
        """
        :type resolve_tuple: ResolveTuple
        :param resolve_tuple:
        :return:
        """
        user_name, resolved_with, resolved_amount = resolve_tuple.user.lower(), resolve_tuple.resolve_with.lower(), resolve_tuple.resolve_amount
        logger.info("Resolving transaction between: user=[{}], resolvedWith=[{}]".format(user_name, resolved_with))
        logger.info("Updating user in database: user=[{}]".format(user_name))
        expresson = "SET resolved = :resolved_val, resolved_with = :resolved_with"
        first_attributes = {':resolved_val': True, ':resolved_with': resolved_with}
        second_attributes = {':resolved_val': True, ':resolved_with': user_name}
        if resolved_amount is not None:
            expresson += ", amount = amount - :resolved_amount"
            first_attributes[':resolved_amount'] = int(resolved_amount)
            second_attributes[':resolved_amount'] = int(resolved_amount)
        else:
            expresson += ", amount = :resolved_amount"
            first_attributes[':resolved_amount'] = 0
            second_attributes[':resolved_amount'] = 0

        self._ticketbot_table.update_item(
            Key={'user_name': user_name},
            UpdateExpression=expresson,
            ExpressionAttributeValues=first_attributes
        )
        logger.info("Updating user in database: user=[{}]".format(resolved_with))
        self._ticketbot_table.update_item(
            Key={'user_name': resolved_with},
            UpdateExpression=expresson,
            ExpressionAttributeValues=second_attributes
        )

    def _update_db_item(self, user_name, operator_string, **items_to_update):
        update_expression = ",".join("SET {value} {operator} :{value}".format(value=item, operator=operator_string) for item in items_to_update)
        expression_attribute_values = {':{}'.format(item): new_value for item, new_value in items_to_update.items()}
        self._ticketbot_table.update_item(
            Key=self._get_user_key(user_name),
            UpdateExpression=update_expression,
            ExpressionAttributeValues=expression_attribute_values
        )

    def _set_user_ticket_amount(self, user_name, new_amount):
        logger.info("Updating ticket amount in database: user=[{}], newAmount=[{}]".format(user_name, new_amount))
        self._update_db_item(user_name, "=", amount=new_amount)

    def _increment_db_item_value(self, user_name, value_name, delta):
        to_update = {value_name: delta}
        self._update_db_item(user_name, "= {} +".format(value_name), **to_update)

    def _change_user_ticket_amount(self, user_name, delta):
        self._increment_db_item_value(user_name, 'amount', delta)

    def _get_users_by_ceremony_date(self, ceremony_date_string, has_tickets_only=True):
        filter_expression = Attr('date').eq(ceremony_date_string)
        if has_tickets_only:
            filter_expression = filter_expression & Attr('amount').gt(0)
        response = self._ticketbot_table.scan(
            FilterExpression=filter_expression
        )
        return response['Items']

    def _get_user_by_username(self, user_name):
        response = self._ticketbot_table.query(
            KeyConditionExpression=Key('user_name').eq(user_name.lower())
        )
        return response['Items']

    def _get_users_by_date_and_operation(self, ceremony_date_string, operation, has_tickets=True):
        filter_expression = Attr('date').eq(ceremony_date_string) & Attr('operation').eq(operation)
        if has_tickets:
            filter_expression = filter_expression & Attr('amount').gt(0)
        response = self._ticketbot_table.scan(
            FilterExpression=filter_expression
        )
        return response['Items']

    def _get_buyers_by_date(self, ceremony_date_string, has_tickets=True):
        return self._get_users_by_date_and_operation(ceremony_date_string, 'buy', has_tickets)

    def _get_sellers_by_date(self, ceremony_date_string, has_tickets=True):
        return self._get_users_by_date_and_operation(ceremony_date_string, 'sell', has_tickets)

    def _send_invalid_ceremony_date_message(self, err):
        logger.info("Sending invalid ceremony date message: user=[{}], givenDate=[{}]".format(err.user, err.given_date))
        message = "Uh oh! Your last command included a date on which there are no ceremonies scheduled.\n\n" \
                  "Your last command was: `{} {} {}`\n\n" \
                  "Here are your choices:\n\n".format(err.operation, err.amount, err.given_date)
        for date in self.ceremony_dict.keys():
            message += "* {}\n".format(date)
        message += "\n\nYou may try again with one of those dates."
        subject = "Invalid Ceremony Date in Your Command"
        self.r.send_message(err.user, subject, message)

    def _send_invalid_command_message(self, err):
        logger.info("Sending invalid command message: user=[{}]".format(err.user))
        message = "Uh oh! You send an invalid command in your message. Here is what you sent me:\n\n"
        for line in err.message_body.split('\n'):
            message += ">{}\n".format(line)
        message += "\n\nTry another command."
        subject = "Invalid Command"
        self.r.send_message(err.user, subject, message)

    def _send_resolve_confirmation_message(self, resolve_tuple):
        """
        :type resolve_tuple: ResolveTuple
        :param resolve_tuple:
        :return:
        """
        user, resolved_with, resolved_amount = resolve_tuple.user, resolve_tuple.resolve_with, resolve_tuple.resolve_amount

        to_append_template = " for {} tickets." if resolved_amount is not None \
            else ". Since no ticket amount was specified, I will assume you bought or sold all of their/your tickets. " \
                 "If this is not accurate, please delete your record with me (`!FAUbot delete me`) and create a new " \
                 "one with your updated buy/sell amount."
        message_template = "You have successfully resolved a transaction with /u/{}{}"
        subject_template = "Successfully resolved with /u/{}"

        # notify first user
        subject = subject_template.format(resolved_with)
        message = message_template.format(resolved_with, (to_append_template if resolved_amount is None
                                                          else to_append_template.format(resolved_amount)))
        self.r.send_message(user, subject, message)

        # notify second user
        subject = subject_template.format(user)
        message = message_template.format(user, (to_append_template if resolved_amount is None
                                                 else to_append_template.format(resolved_amount)))
        self.r.send_message(resolved_with, subject, message)

    def _send_confirmation_message(self, command_tuple):
        """
        :type command_tuple: CommandTuple
        :param command_tuple:
        :return:
        """
        logger.info("Sending confirmation message: user=[{}]".format(command_tuple.user))
        message = "Your request has been processed. Your command:\n\n" \
                  "`{} {} {}`\n\n" \
                  "You're in the database now.".format(command_tuple.operation, command_tuple.amount, command_tuple.date)
        subject = "Successfully Processed Request"
        self.r.send_message(command_tuple.user, subject, message)

    def _send_delete_confirmation_message(self, user_name):
        message = "Your graduation ticket record has been deleted from the database. You will no longer be considered " \
                  "when I try to match buyers and sellers of graduation tickets. Feel free to sign up again at any time."
        subject = "Successfully Deleted Record"
        self.r.send_message(user_name, subject, message)

    def _send_missing_ceremony_message(self, err):
        message = "Oh no! You want to {} {} tickets, but you didn't specify which ceremony you wish to attend. " \
                  "Please send another command and include the ceremony date. Your choices are:\n\n".format(err.operation, err.amount)
        for date in self.ceremony_dict.keys():
            message += "* {}\n".format(date)
        message += '\n\n'
        message += "I recommend copying and pasting one of those dates when you write your new command so that I " \
                   "don't get confused and put the wrong date by your name in my database. I'm usually pretty good " \
                   "about parsing dates, but better safe than sorry!"
        subject = "Missing Ceremony in Command"
        self.r.send_message(err.user, subject, message)

    def _parse_new_command(self, command, message):
        user = message.author.name.lower()
        operation = command.groups()[0]
        amount = command.groups()[1]
        try:
            dt = parse(command.groups()[2])
        except ValueError:
            raise InvalidCeremonyDate(user, operation, amount, command.groups()[2], self.ceremony_dict.keys())
        except (IndexError, AttributeError):
            raise MissingCeremonyDate(user, operation, amount, self.ceremony_dict.keys())
        else:
            try:
                date = next((d for d in self.ceremony_dict if parse(d) == dt))
            except StopIteration:
                raise InvalidCeremonyDate(user, operation, amount, command.groups()[2], self.ceremony_dict.keys())
        return CommandTuple(user=user, operation=operation, amount=amount, date=date)

    @staticmethod
    def _parse_resolve_command(command, message):
        resolved_with = command.groups()[0]
        try:
            resolved_amount = command.groups()[1]
        except IndexError:
            resolved_amount = None
        return ResolveTuple(user=message.author.name, resolve_with=resolved_with, resolve_amount=resolved_amount)

    def parse_command(self, message):
        logger.info("Parsing message: author=[{}], message=[{}]".format(message.author.name, message.body))
        if "!FAUbot" not in message.body:
            raise NoCommandInMessage
        command = self._command_regex.match(message.body)
        if command:
            return self._parse_new_command(command, message)
        command = self._resolve_regex.match(message.body)
        if command:
            return self._parse_resolve_command(command, message)
        raise InvalidCommand(message.author, message.body)

    def work(self):
        logger.info("Getting unread messages")
        inbox = self.r.get_unread(unset_has_mail=True)
        to_notify_params = {}
        to_notify_fns = {}
        for message in inbox:
            mark_as_read = True
            if self.DELETE_COMMAND in message.body:
                self._new_user_delete_queue(message.author.name.lower())
                to_notify_params[message.author.name.lower()] = message.author.name.lower()
            else:
                try:
                    command = self.parse_command(message)
                    pass
                except NoCommandInMessage:
                    mark_as_read = False
                except InvalidCommand as err:
                    to_notify_params[err.user.lower()] = err
                    to_notify_fns[err.user.lower()] = self._send_invalid_command_message
                except InvalidCeremonyDate as err:
                    to_notify_params[err.user] = err
                    to_notify_fns[err.user.lower()] = self._send_invalid_ceremony_date_message
                except MissingCeremonyDate as err:
                    to_notify_params[err.user.lower()] = err
                    to_notify_fns[err.user.lower()] = self._send_missing_ceremony_message
                else:
                    if isinstance(command, CommandTuple):
                        self._new_user_add_queue(command)
                        to_notify_params[message.author.name.lower()] = command
                        pass
                    if isinstance(command, ResolveTuple):
                        self._set_user_resolved(command)
                        self._send_resolve_confirmation_message(command)
            if mark_as_read:
                message.mark_as_read()
        to_notify_fns.update(self._batch_write_new_db_records())
        for user_name, notify_fn in to_notify_fns.items():
            notify_fn(to_notify_params[user_name])


class NoCommandInMessage(Exception):
    pass


class InvalidCommand(ValueError):
    def __init__(self, user, message_body):
        self.user = user
        self.message_body = message_body
        msg = "Invalid TicketBot command. Message:\n\n{}".format(message_body)
        super(InvalidCommand, self).__init__(msg)


class InvalidCeremonyDate(ValueError):
    def __init__(self, user, operation, amount, given_date, choices):
        self.user = user
        self.amount = amount
        self.operation = operation
        self.given_date = given_date
        msg = "Invalid Ceremony Date: given=[{}], choices={}".format(self.given_date, choices)
        super(InvalidCeremonyDate, self).__init__(msg)


class MissingCeremonyDate(ValueError):
    def __init__(self, user, operation, amount, choices):
        self.user = user
        self.amount = amount
        self.operation = operation
        msg = "Missing ceremony date in command: user=[{}], amount=[{}], operation=[{}], choices=[{}]".format(user, operation, amount, choices)
        super(MissingCeremonyDate, self).__init__(msg)


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
