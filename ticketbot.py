import re
import requests
import datetime
import boto3, boto3.dynamodb
from boto3.dynamodb.conditions import Key, Attr
from collections import OrderedDict, namedtuple
from dateutil.parser import parse
from pprint import PrettyPrinter
from bs4 import BeautifulSoup
from queue import Queue

from bots import RedditBot
from mixins import RedditPosterMixin
from config import getLogger
from config.bot_config import get_notify_interval

p = PrettyPrinter(indent=2)
logger = getLogger()


# region ticketbot
class TicketBot(RedditBot, RedditPosterMixin):
    NOTIFY_INTERVAL = get_notify_interval()
    CEREMONY_URL = "https://www.fau.edu/registrar/graduation/ceremony.php"
    CommandTuple = namedtuple('CommandTuple', 'user operation amount date')
    ResolveTuple = namedtuple('ResolveTuple', 'user resolve_with resolve_amount')
    NotifyFunctionTuple = namedtuple('NotifyFunctionTuple', 'function params')
    SetUserNotifiedTuple = namedtuple('SetUserNotifiedTuple', 'user ceremony_date notify_date')
    DELETE_COMMAND = "!FAUbot delete me"
    TRIGGER = ""  # Don't use characters that need to be escaped in regular expressions
    COMMAND_PATTERN = "^!FAUbot (buy|sell) (\d{1,2})(?: (.+))?$"
    RESOLVE_PATTERN = "^!FAUbot resolve (\d{1,2}) (?:\/u\/)?([\w_-]{3,})$"

    def __init__(self, user_name, *args, **kwargs):
        super().__init__(user_name, *args, **kwargs)
        self._command_regex = re.compile(self.COMMAND_PATTERN)
        self._resolve_regex = re.compile(self.RESOLVE_PATTERN)
        self._ticketbot_table = self._get_ticketbot_table()
        self.ceremony_dict = self.get_ceremony_dict()
        self.add_queue = Queue()
        self.delete_queue = Queue()

    # region Ceremony-Functions
    @staticmethod
    def _get_ceremony_data():
        r = requests.get(TicketBot.CEREMONY_URL)
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
    # endregion

    # region DB-Helpers
    @staticmethod
    def _get_ticketbot_table():
        db = boto3.resource('dynamodb')
        return db.Table('ticketbot')

    @staticmethod
    def _get_user_item(command_tuple):
        """
        :type command_tuple: TicketBot.CommandTuple
        :param command_tuple:
        :return:
        """
        return {
            'user_name': command_tuple.user.lower(),
            'ceremony_date': command_tuple.date,
            'operation': command_tuple.operation,
            'amount': command_tuple.amount,
            'resolved': False
        }

    @staticmethod
    def _get_user_key(user_name):
        return {'user_name': user_name.lower()}

    def _add_new_db_record(self, command_tuple):
        """
        :type command_tuple: TicketBot.CommandTuple
        :param command_tuple:
        :return:
        """
        logger.info("Saving new record in database: {}".format(command_tuple))
        self._ticketbot_table = self._get_ticketbot_table()
        self._ticketbot_table.put_item(Item=self._get_user_item(command_tuple))

    def _delete_db_record(self, user_name):
        logger.info("Deleting user from database: user=[{}]".format(user_name))
        self._ticketbot_table = self._get_ticketbot_table()
        self._ticketbot_table.delete_item(Key=self._get_user_key(user_name))

    def _update_db_record(self, user_name, operator_string, **items_to_update):
        update_expression = ",".join("SET {value} {operator} :{value}".format(value=item, operator=operator_string) for item in items_to_update)
        expression_attribute_values = {':{}'.format(item): new_value for item, new_value in items_to_update.items()}
        self._ticketbot_table = self._get_ticketbot_table()
        self._ticketbot_table.update_item(
            Key=self._get_user_key(user_name),
            UpdateExpression=update_expression,
            ExpressionAttributeValues=expression_attribute_values
        )

    def _increment_db_item_value(self, user_name, value_name, delta):
        to_update = {value_name: delta}
        self._update_db_record(user_name, "= {} +".format(value_name), **to_update)

    def _batch_write_new_db_records(self):
        if self._queue_data_present():
            self._ticketbot_table = self._get_ticketbot_table()
            with self._ticketbot_table.batch_writer(overwrite_by_pkeys=['user_name', 'ceremony_date']) as batch:
                while not self.delete_queue.empty():
                    item = self.delete_queue.get()
                    logger.info("Deleting DB record: Key=[{}]".format(item))
                    batch.delete_item(Key=item)
                while not self.add_queue.empty():
                    item = self.add_queue.get()
                    logger.info("Writing new DB record: Item=[{}]".format(item))
                    batch.put_item(item)
        else:
            logger.info("All Queues are empty. Not writing anything to database.")

    def _set_user_notified(self, notify_tuple):
        """
        :type notify_tuple: TicketBot.SetUserNotifiedTuple
        :param notify_tuple:
        """
        table = self._get_ticketbot_table()
        table.update_item(
            Key={
                'user_name': notify_tuple.user,
                'ceremony_date': notify_tuple.ceremony_date
            },
            UpdateExpression='SET last_notified = :nd',
            ExpressionAttributeValues={':nd': notify_tuple.notify_date}
        )

    def _set_users_notified(self, user_dict):
        now = datetime.datetime.utcnow().isoformat()
        for ceremony_date, operation_dict in user_dict.items():
            all_users = operation_dict['buy'] + operation_dict['sell']
            for user in all_users:
                notify_tuple = TicketBot.SetUserNotifiedTuple(user=user['user_name'], ceremony_date=ceremony_date, notify_date=now)
                self._set_user_notified(notify_tuple)
    # endregion

    # region Command-Queue
    def _new_user_add_queue(self, command_tuple):
        """
        :type command_tuple: TicketBot.CommandTuple
        :param command_tuple:
        :return:
        """
        self.add_queue.put(self._get_user_item(command_tuple))

    def _new_user_delete_queue(self, user_name):
        self.delete_queue.put(self._get_user_key(user_name))
    # endregion

    # region Set-User-Vals
    def _set_user_resolved(self, resolve_tuple):  # todo refactor
        """
        :type resolve_tuple: TicketBot.ResolveTuple
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

    def _set_user_ticket_amount(self, user_name, new_amount):
        logger.info("Updating ticket amount in database: user=[{}], newAmount=[{}]".format(user_name, new_amount))
        self._update_db_record(user_name, "=", amount=new_amount)



    def _change_user_ticket_amount(self, user_name, delta):
        self._increment_db_item_value(user_name, 'amount', delta)
    # endregion

    # region Get-Users
    def _get_users_by_ceremony_date(self, ceremony_date_string):
        self._ticketbot_table = self._get_ticketbot_table()
        filter_expression = Key('ceremony_date').eq(ceremony_date_string)
        result = []
        response = self._ticketbot_table.scan(
            ProjectionExpression='user_name, ceremony_date, amount, operation, last_notified',
            FilterExpression=filter_expression)
        result = response['Items']
        while 'LastEvaluatedKey' in response:
            response = self._ticketbot_table.scan(
                ProjectionExpression="user_name, ceremony_date, amount, operation, last_notified",
                FilterExpression=filter_expression,
                ExclusiveStartKey=response['LastEvaluatedKey'])
            result.extend(response['Items'])
        return result

    def _get_user_by_username(self, user_name):
        self._ticketbot_table = self._get_ticketbot_table()
        response = self._ticketbot_table.query(
            KeyConditionExpression=Key('user_name').eq(user_name.lower())
        )
        return response['Items']

    def _get_users_by_date_and_operation(self, ceremony_date_string, operation, has_tickets=True):
        users = self._get_users_by_ceremony_date(ceremony_date_string)
        users = list(filter(lambda u: u['operation'] == operation, users))
        return users

    def _has_last_notified(self, user):
        n = user.get('last_notified', None)
        return n is not None

    def _get_users_by_date_and_operations(self, ceremony_date_string, last_notify_hours=0):
        users = self._get_users_by_ceremony_date(ceremony_date_string)
        result = {'buy': [u for u in users if u['ceremony_date'] == ceremony_date_string and u['operation'] == 'buy'],
                  'sell': [u for u in users if u['ceremony_date'] == ceremony_date_string and u['operation'] == 'sell']}
        if last_notify_hours > 0:
            now = datetime.datetime.utcnow()
            limit = now - datetime.timedelta(hours=last_notify_hours)
            for key, val in result.items():
                result[key] = [u for u in val if not self._has_last_notified(u) or parse(u['last_notified']) <= limit]
        return result

    def _get_buyers_by_date(self, ceremony_date_string, has_tickets=True):
        return self._get_users_by_date_and_operation(ceremony_date_string, 'buy', has_tickets)

    def _get_sellers_by_date(self, ceremony_date_string, has_tickets=True):
        return self._get_users_by_date_and_operation(ceremony_date_string, 'sell', has_tickets)

    def get_buyers_for_notification(self):
        return {ceremony: self._get_buyers_by_date(ceremony) for ceremony in self.ceremony_dict}

    def get_sellers_for_notification(self):
        return {ceremony: self._get_sellers_by_date(ceremony) for ceremony in self.ceremony_dict}

    def get_users_for_notification(self):
        users = {ceremony: self._get_users_by_date_and_operations(ceremony, TicketBot.NOTIFY_INTERVAL) for ceremony in self.ceremony_dict}
        to_return = {ceremony: operation_dict for ceremony, operation_dict in users.items() if any(val for val in operation_dict.values())}
        return to_return
    # endregion

    # region Reddit-Messages
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
        :type resolve_tuple: TicketBot.ResolveTuple
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
        :type command_tuple: TicketBot.CommandTuple
        :param command_tuple:
        :return:
        """
        logger.info("Sending confirmation message: user=[{}]".format(command_tuple.user))

        message = """Your request has been processed. Here is your profile:

* **User:** {user}
* **Date:** {date}
* **Operation:** {operation}
* **Amount:** {amount}


I will try to find other users who can help you {operation} those tickets. If I find any, I'll send you a list of links
to their Reddit user profiles. From there you can send them private messages to discuss {operation}ing the tickets.
""".format(user=command_tuple.user, date=command_tuple.date, operation=command_tuple.operation, amount=command_tuple.amount)

        subject = "Successfully Processed Request"
        self.r.send_message(command_tuple.user, subject, message)

    def _send_delete_confirmation_message(self, user_name):
        message = "Your graduation ticket record has been deleted from the database. " \
                  "You will no longer be considered when I try to match buyers and sellers of graduation tickets. " \
                  "Feel free to sign up again at any time."
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

    def _generate_buy_sell_notification(self, operation):

        message_template = "Good news! I found some students who are trying to {other_operation} graduation tickets. " \
                           "Now you should visit their profiles and send them private messages to discuss {operation}ing " \
                           "the tickets.\n\nIf you end up {operation}ing tickets from anyone, please let me know! Here's " \
                           "how you \"resolve\" a purchase. :\n\n `!Faubot resolve <number> <{other_operation}er_username>`\n\n" \
                           "For example, `!FAUbot resolve 5 /u/jpfau` means you {past_tense_operation} 5 tickets from " \
                           "the user jpfau. For now, I will only accept resolve commands from buyers, and I will ignore " \
                           "them from sellers. It's up to you to help keep my ticket system working!\n\nAnyway, here is " \
                           "the list of {other_operation}ers:\n\n"
        if operation == 'buy':
            past_tense_operation = 'bought'
            other_operation = 'sell'
        elif operation == 'sell':
            past_tense_operation = 'sold'
            other_operation = 'buy'
        else:
            raise ValueError("Parameter not 'buy' or 'sell': parameter=[{}]".format(operation))
        return message_template.format(operation=operation, past_tense_operation=past_tense_operation, other_operation=other_operation)

    def _notify_buyers_sellers(self, to_notify, operation):
        for user_name, list_users in to_notify.items():
            message = self._generate_buy_sell_notification(operation)
            for other_user in list_users:
                message += "* **/u/{}** is {}ing **{}** tickets\n".format(other_user['user_name'], other_user['operation'], other_user['amount'])
            message += "\n\n"
            subject = "It's a match!"
            self.r.send_message(user_name, subject, message)

    def notify_buyers(self, buyers):
        self._notify_buyers_sellers(buyers, 'buy')

    def notify_sellers(self, sellers):
        self._notify_buyers_sellers(sellers, 'sell')
    # endregion

    # region Parse-Commands
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
        return TicketBot.CommandTuple(user=user, operation=operation, amount=amount, date=date)

    @staticmethod
    def _parse_resolve_command(command, message):
        resolved_with = command.groups()[0]
        try:
            resolved_amount = command.groups()[1]
        except IndexError:
            resolved_amount = None
        return TicketBot.ResolveTuple(user=message.author.name, resolve_with=resolved_with, resolve_amount=resolved_amount)

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

    def parse_ticket_commands(self):
        to_notify = []
        inbox = self.r.get_unread(unset_has_mail=True)
        for message in inbox:

            mark_as_read = True
            function, params = None, None
            if self.DELETE_COMMAND in message.body:
                user = message.author.name.lower()
                self._new_user_delete_queue(user)
                function, params = self._send_delete_confirmation_message, user
            else:
                try:
                    command = self.parse_command(message)
                except NoCommandInMessage:
                    mark_as_read = False
                except InvalidCommand as err:
                    function, params = self._send_invalid_command_message, err
                except InvalidCeremonyDate as err:
                    function, params = self._send_invalid_ceremony_date_message, err
                except MissingCeremonyDate as err:
                    function, params = self._send_missing_ceremony_message, err
                else:
                    if isinstance(command, TicketBot.CommandTuple):
                        self._new_user_add_queue(command)
                        function, params = self._send_confirmation_message, command
                    if isinstance(command, TicketBot.ResolveTuple):
                        self._set_user_resolved(command)
                        function, params = self._send_resolve_confirmation_message, command
            if mark_as_read:
                message.mark_as_read()
                to_notify.append(TicketBot.NotifyFunctionTuple(function=function, params=params))
        self._batch_write_new_db_records()
        for nt in to_notify:
            nt.function(nt.params)
    #endregion

    def match_ticket_users(self):
        users = self.get_users_for_notification()
        for operation_dict in users.values():
            buyers, sellers = operation_dict['buy'], operation_dict['sell']
            buyers_to_notify = {buyer['user_name']: sorted(sellers, key=lambda s: s['amount']) for buyer in buyers}
            sellers_to_notify = {seller['user_name']: sorted(buyers, key=lambda b: b['amount']) for seller in sellers}
            self.notify_buyers(buyers_to_notify)
            self.notify_sellers(sellers_to_notify)
        self._set_users_notified(users)

    def update_ticket_megathread(self):
        pass # todo implement

    def work(self):
        self.parse_ticket_commands()
        self.match_ticket_users()
        self.update_ticket_megathread()
# endregion


# region Exceptions
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
# endregion


# region Main
def main():
    from config.praw_config import get_all_site_names
    from argparse import ArgumentParser
    parser = ArgumentParser("Running TicketBot by itself")
    parser.add_argument("-u", "--user", dest="reddit_user", required=True, choices=get_all_site_names(),
                        help="Specify which Reddit user from praw.ini to use.")
    args = parser.parse_args()
    test = TicketBot(args.reddit_user, run_once=True)
    test.start()
    test.stop_event.wait()


if __name__ == '__main__':
    main()
# endregion
