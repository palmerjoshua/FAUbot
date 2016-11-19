import re

import cachetools
import requests
import datetime
import boto3, boto3.dynamodb
from boto3.dynamodb.conditions import Key, Attr
from collections import OrderedDict, namedtuple
from dateutil.parser import parse
from pprint import PrettyPrinter
from bs4 import BeautifulSoup
from queue import Queue
from pymongo import MongoClient
from bots import RedditBot
from mixins import MarkdownMaker, RedditPosterMixin
from config import getLogger
from config.bot_config import get_notify_interval

p = PrettyPrinter(indent=2)
logger = getLogger()


# region ticketbot
class TicketBot(RedditPosterMixin):
    # region Static-Variables
    NOTIFY_INTERVAL = get_notify_interval()
    CEREMONY_URL = "https://www.fau.edu/registrar/graduation/ceremony.php"
    UserTuple = namedtuple('UserTuple', 'user operation amount date')
    ResolveTuple = namedtuple('ResolveTuple', 'user resolve_with resolve_amount date')
    NotifyFunctionTuple = namedtuple('NotifyFunctionTuple', 'function params')
    SetUserNotifiedTuple = namedtuple('SetUserNotifiedTuple', 'user ceremony_date notify_date')
    DELETE_COMMAND = "!FAUbot delete me"
    TRIGGER = ""  # Don't use characters that need to be escaped in regular expressions
    COMMAND_PATTERN = "^!FAUbot (buy|sell) (\d{1,2})(?: (.+))?$"
    RESOLVE_PATTERN = "^!FAUbot resolve (\d{1,2}) (?:\/u\/)?([\w_-]{3,})(?: (.+))?$"
    # endregion

    def __init__(self, user_name, *args, **kwargs):
        super().__init__(user_name, *args, **kwargs)
        self._command_regex = re.compile(TicketBot.COMMAND_PATTERN)
        self._resolve_regex = re.compile(TicketBot.RESOLVE_PATTERN)
        self._db_client = MongoClient('localhost', 1234)  # todo config
        self._db = self._db_client[self.__class__.__name__]
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

    @cachetools.ttl_cache(ttl=1800)
    def get_ceremony_dict(self):
        data = TicketBot._get_ceremony_data()
        return TicketBot._get_ceremony_dict(data)

    def refresh_ceremony_dict(self):
        self.ceremony_dict = self.get_ceremony_dict()

    def get_current_season(self):
        self.refresh_ceremony_dict()
        ceremony_date_strings = list(self.ceremony_dict.keys())
        year = datetime.datetime.now().year
        graduation_month = parse(ceremony_date_strings[0]).month
        if graduation_month in (12, 11, 1):
            season = "Fall"
        elif graduation_month in (5, 4, 6):
            season = "Spring"
        else:
            season = "Summer"
        return "{} {}".format(season, year)
    # endregion

    # region DB-Helpers
    @cachetools.ttl_cache(ttl=10)
    def _get_ticketbot_table(self):
        db = boto3.resource('dynamodb')
        return db.Table('ticketbot')


    def _get_user_item(self, command_tuple):
        """
        :type command_tuple: TicketBot.CommandTuple
        :param command_tuple:
        :return:
        """
        item = self._get_user_key(command_tuple.user, command_tuple.date)
        item.update({
            'operation': command_tuple.operation,
            'amount': command_tuple.amount,
            'resolved': False
        })
        return item

    def _get_user_key(self, user_name, ceremony_date):
        return {'user_name': user_name.lower(), 'ceremony_date': next(cd for cd in self.ceremony_dict if parse(cd) == parse(ceremony_date))}

    def _add_new_db_record(self, command_tuple):
        """
        :type command_tuple: TicketBot.CommandTuple
        :param command_tuple:
        :return:
        """
        logger.info("Saving new record in database: {}".format(command_tuple))
        ticketbot_table = self._get_ticketbot_table()
        ticketbot_table.put_item(Item=self._get_user_item(command_tuple))

    def _add_new_user(self, command_tuple):
        user = self._get_user_item(command_tuple)
        self._db.users.insert_one(user)

    def _delete_user(self, user_name, ceremony_date=None, operation=None):
        query = {'user_name': user_name}
        if ceremony_date:
            query['ceremony_date'] = ceremony_date
        if operation:
            query['operation'] = operation
        self._db.users.delete_one(query)


    def _delete_db_record(self, user_name, ceremony_date):
        logger.info("Deleting user from database: user=[{}]".format(user_name))
        ticketbot_table = self._get_ticketbot_table()
        ticketbot_table.delete_item(Key=self._get_user_key(user_name, ceremony_date))

    def get_update_expression(self, operator, operands):
        return "SET " + ", ".join("{value} {operator} :inc".format(value=item, operator=operator) for item in operands)

    def get_expression_attributes(self, items_to_update):
        return {':inc': new_value for item, new_value in items_to_update.items()}

    def _update_db_record(self, user_name, ceremony_date, operator_string, **items_to_update):
        update_expression = self.get_update_expression(operator_string, items_to_update)
        expression_attribute_values = self.get_expression_attributes(items_to_update)
        ticketbot_table = self._get_ticketbot_table()
        ticketbot_table.update_item(
            Key=self._get_user_key(user_name, ceremony_date),
            UpdateExpression=update_expression,
            ExpressionAttributeValues=expression_attribute_values
        )

    def _update_amount(self, user_name, ceremony_date, delta):
        user = self._db.users.find_one({'user_name': user_name, 'ceremony_date': ceremony_date})
        amount = int(user['amount'])
        self._db.users.update({'_id': user['_id']}, {'$set': {'amount': amount + delta}})

    def increment_amount(self, user_name, ceremony_date, increment):
        self._update_amount(user_name, ceremony_date, increment)

    def decrement_amount(self, user_name, ceremony_date, decrement):
        self._update_amount(user_name, ceremony_date, -decrement)

    def _increment_db_item_value(self, user_name, ceremony_date, value_name, delta):
        to_update = {value_name: delta}
        self._update_db_record(user_name, ceremony_date, "= {} +".format(value_name), **to_update)

    def _decrement_db_item_value(self, user_name, ceremony_date, value_name, delta):
        to_update = {value_name: delta}
        self._update_db_record(user_name, ceremony_date, "= {} -".format(value_name), **to_update)

    def _batch_write_new_users(self):
        if self._queue_data_present():
            users = self._db.users
            new_users = []
            while not self.add_queue.empty():
                new_users.append(self.add_queue.get())
            users.insert_many(new_users)

    def _batch_delete_users(self):
        while not self.delete_queue.empty():
            user_id = self.delete_queue.get()['_id']
            self._db.users.remove(user_id)

    def _batch_write_new_db_records_new(self):
        self._batch_write_new_users()
        self._batch_delete_users()

    def _batch_write_new_db_records(self):
        if self._queue_data_present():
            ticketbot_table = self._get_ticketbot_table()
            with ticketbot_table.batch_writer(overwrite_by_pkeys=['user_name', 'ceremony_date']) as batch:
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

    def _set_user_notified_new(self, notify_tuple):
        user = self._db.users.find_one({'user_name': notify_tuple.user, 'ceremony_date': notify_tuple.ceremony_date})
        if user:
            self._db.users.update({'_id': user['_id']}, {'$set': {'last_notified': notify_tuple.notify_date}})
        else:
            logger.error("User not found")

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

    def _set_users_notified_new(self, user_dict):
        now = datetime.datetime.utcnow().isoformat()
        for ceremony_date, operation_dict in user_dict.items():
            all_users = operation_dict['buy'] + operation_dict['sell']
            for user in all_users:
                notify_tuple = TicketBot.SetUserNotifiedTuple(user=user['user_name'], ceremony_date=ceremony_date,
                                                              notify_date=now)
                self._set_user_notified_new(notify_tuple)

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
        key = {'user_name': user_name}
        self.delete_queue.put(key)
    # endregion

    # region Set-User-Vals

    def _resolve_user(self, resolve_tuple):
        user_name, resolved_with = resolve_tuple.user.lower(), resolve_tuple.resolve_with.lower()
        resolved_amount, ceremony_date = int(resolve_tuple.resolve_amount), resolve_tuple.date
        self.decrement_amount(user_name, resolve_tuple.date, resolved_amount)
        self.decrement_amount(resolved_with, resolve_tuple.date, resolved_amount)


    def _set_user_resolved(self, resolve_tuple, _ticketbot_table=None):  # todo refactor
        """
        :type resolve_tuple: TicketBot.ResolveTuple
        :param resolve_tuple:
        :return:
        """
        user_name, resolved_with = resolve_tuple.user.lower(), resolve_tuple.resolve_with.lower()
        resolved_amount, ceremony_date = int(resolve_tuple.resolve_amount), resolve_tuple.date

        logger.info("Resolving transaction between: user=[{}], resolvedWith=[{}]".format(user_name, resolved_with))
        logger.info("Updating user in database: user=[{}]".format(user_name))
        self._decrement_db_item_value(user_name, resolve_tuple.date, 'amount', resolved_amount)
        logger.info("Updating user in database: user=[{}]".format(resolved_with))
        self._decrement_db_item_value(resolved_with, resolve_tuple.date, 'amount', resolved_amount)

    def _set_user_ticket_amount(self, user_name, ceremony_date, new_amount):
        logger.info("Updating ticket amount in database: user=[{}], newAmount=[{}]".format(user_name, new_amount))
        self._update_db_record(user_name, ceremony_date, "=", amount=new_amount)

    def _change_user_ticket_amount(self, user_name, ceremony_date, delta):
        db_function = self._decrement_db_item_value if delta < 0 else self._increment_db_item_value
        db_function(user_name, ceremony_date, 'amount', abs(delta))
    # endregion

    # region Get-Users

    def _get_users_by_ceremony_date_new(self, ceremony_date_string):
        return self._db.users.find_many({'ceremony_date': {'$eq': ceremony_date_string}})

    def _get_users_by_ceremony_date(self, ceremony_date_string):
        ticketbot_table = self._get_ticketbot_table()
        filter_expression = Key('ceremony_date').eq(ceremony_date_string)
        result = []
        response = ticketbot_table.scan(
            ProjectionExpression='user_name, ceremony_date, amount, operation, last_notified',
            FilterExpression=filter_expression)
        result = response['Items']
        while 'LastEvaluatedKey' in response:
            response = ticketbot_table.scan(
                ProjectionExpression="user_name, ceremony_date, amount, operation, last_notified",
                FilterExpression=filter_expression,
                ExclusiveStartKey=response['LastEvaluatedKey'])
            result.extend(response['Items'])
        return result

    def _get_user_by_username(self, user_name):
        ticketbot_table = self._get_ticketbot_table()
        response = ticketbot_table.query(
            KeyConditionExpression=Key('user_name').eq(user_name.lower())
        )
        return response['Items']

    def _get_users_by_date_and_operation_new(self, ceremony_date_string, operation, has_tickets=True):
        find_dict = {'ceremony_date': ceremony_date_string, 'operation': operation}
        #if has_tickets:
            #find_dict['amount'] = {'$gt': 0}

        users = [user for user in self._db.users.find(find_dict)]
        if has_tickets:
            users = [user for user in users if int(user['amount']) > 0]
        return users

    def _get_users_by_date_and_operation(self, ceremony_date_string, operation, has_tickets=True):
        users = self._get_users_by_ceremony_date(ceremony_date_string)
        users = list(filter(lambda u: u['operation'] == operation, users))
        return users

    def _has_last_notified(self, user):
        n = user.get('last_notified', None)
        return n is not None

    def _get_users_by_date_and_operations_new(self, ceremony_date_string, last_notify_hours=0):
        operation_dict = {'buy': self._get_users_by_date_and_operation_new(ceremony_date_string, 'buy'),
                          'sell': self._get_users_by_date_and_operation_new(ceremony_date_string, 'sell')}
        if last_notify_hours > 0:
            now = datetime.datetime.utcnow()
            limit = now - datetime.timedelta(hours=last_notify_hours)
            for key, val in operation_dict.items():
                operation_dict[key] = [u for u in val if not self._has_last_notified(u) or parse(u['last_notified']) <= limit]
        return operation_dict

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

    def _get_buyers_by_date_new(self, ceremony_date_string, has_tickets=True):
        return self._get_users_by_date_and_operation_new(ceremony_date_string, 'buy', has_tickets)

    def _get_buyers_by_date(self, ceremony_date_string, has_tickets=True):
        return self._get_users_by_date_and_operation(ceremony_date_string, 'buy', has_tickets)

    def _get_sellers_by_date_new(self, ceremony_date_string, has_tickets=True):
        return self._get_users_by_date_and_operation_new(ceremony_date_string, 'sell', has_tickets)

    def _get_sellers_by_date(self, ceremony_date_string, has_tickets=True):
        return self._get_users_by_date_and_operation(ceremony_date_string, 'sell', has_tickets)

    def get_buyers_for_notification_new(self):
        buyers = {ceremony: self._get_buyers_by_date_new(ceremony) for ceremony in self.ceremony_dict}
        to_delete = [key for key, val in buyers.items() if not val]
        for key in to_delete:
            del buyers[key]
        return buyers

    def get_buyers_for_notification(self):
        buyers = {ceremony: self._get_buyers_by_date(ceremony) for ceremony in self.ceremony_dict}
        to_delete = [key for key, val in buyers.items() if not val]
        for key in to_delete:
            del buyers[key]
        return buyers

    def get_sellers_for_notification_new(self):
        sellers = {ceremony: self._get_sellers_by_date_new(ceremony) for ceremony in self.ceremony_dict}
        to_delete = [key for key, val in sellers.items() if not val]
        for key in to_delete:
            del sellers[key]
        return sellers

    def get_sellers_for_notification(self):
        sellers = {ceremony: self._get_sellers_by_date(ceremony) for ceremony in self.ceremony_dict}
        to_delete = [key for key, val in sellers.items() if not val]
        for key in to_delete:
            del sellers[key]
        return sellers

    def get_users_for_notification_new(self):
        users = {ceremony: self._get_users_by_date_and_operations_new(ceremony, TicketBot.NOTIFY_INTERVAL) for ceremony in self.ceremony_dict}
        to_return = {ceremony: operation_dict for ceremony, operation_dict in users.items() if
                     any(val for val in operation_dict.values())}
        return to_return

    def get_users_for_notification(self):
        users = {ceremony: self._get_users_by_date_and_operations(ceremony, TicketBot.NOTIFY_INTERVAL) for ceremony in self.ceremony_dict}
        to_return = {ceremony: operation_dict for ceremony, operation_dict in users.items() if any(val for val in operation_dict.values())}
        return to_return
    # endregion

    # region Reddit-Posts

    def megathread_title(self):
        return "Graduation Ticket Megathread [{}]".format(self.get_current_season())

    def get_megathread_by_title(self, subreddit, title=""):
        current_megathread_title = title or self.megathread_title()
        search_query = "title:{} AND author:{}".format(current_megathread_title, self.USER_NAME)
        for post in self.r.search(search_query, subreddit=subreddit):
            if post:
                logger.info("Found megathread by title=[{}], subreddit=[{}]".format(current_megathread_title, subreddit))
                return post
        return None


    def create_ticket_megathread(self, subreddit, title, text):
        return self.create_monitored_post(subreddit, title, text)

    # endregion

    # region Reddit-Messages
    def _send_invalid_ceremony_date_message(self, err, message_object):
        logger.info("Sending invalid ceremony date message: user=[{}], givenDate=[{}]".format(err.user, err.given_date))
        response_text = self.reddit_paragraph("Uh oh! Your last command included a date on which there are no ceremonies "
                                        "scheduled. Your last command was:")
        response_text += self.reddit_code_block("{} {} {}".format(err.operation, err.amount, err.given_date))
        response_text += "Here are your choices:"
        response_text += self.reddit_list(self.ceremony_dict.keys())
        response_text += self.reddit_paragraph("You may try again with one of those dates.")
        message_object.reply(response_text)


    def _send_invalid_command_message(self, err, message_object):
        logger.info("Sending invalid command message: user=[{}]".format(err.user))
        response_text = self.reddit_paragraph("Uh oh! You send an invalid command in your message. Here is what you sent me:")
        response_text += self.reddit_list(err.message_body.split('\n'))
        response_text += self.reddit_paragraph("Try another command.")
        subject = "Invalid Command"
        message_object.reply(response_text)

    def _send_resolve_confirmation_message(self, resolve_tuple):
        """
        :type resolve_tuple: TicketBot.ResolveTuple
        :param resolve_tuple:
        :return:
        """
        user, resolved_with, resolved_amount = resolve_tuple.user, resolve_tuple.resolve_with, resolve_tuple.resolve_amount

        message_template = "You have successfully resolved a transaction with /u/{}"
        if resolved_amount:
            message_template += " for {} tickets.".format(resolved_amount)
        else:
            message_template += ". Since no ticket amount was specified, I will assume you bought or sold all of their tickets. " \
                 "If this is not accurate, please delete your record with the command `!FAUbot delete me` and create a new " \
                 "one with your updated buy/sell amount."

        subject = "Resolved Transaction with /u/{}"
        self.r.send_message(user, subject.format(resolved_with), message_template.format(resolved_with))
        self.r.send_message(resolved_with, subject.format(user), message_template.format(user))


    def _send_confirmation_message(self, command_tuple):
        """
        :type command_tuple: TicketBot.CommandTuple
        :param command_tuple:
        :return:
        """
        logger.info("Sending confirmation message: user=[{}]".format(command_tuple.user))

        response_text = """Your request has been processed. Here is your profile:

* **User:** {user}
* **Date:** {date}
* **Operation:** {operation}
* **Amount:** {amount}


I will try to find other users who can help you {operation} those tickets. If I find any, I'll send you a list of links
to their Reddit user profiles. From there you can send them private messages to discuss {operation}ing the tickets.
""".format(user=command_tuple.user, date=command_tuple.date, operation=command_tuple.operation, amount=command_tuple.amount)

        subject = "Successfully Processed Request"
        self.r.send_message(command_tuple.user, subject, response_text)

    def _send_delete_confirmation_message(self, user_name):
        message = self.reddit_paragraph("Your graduation ticket record has been deleted from the database. "
                                        "You will no longer be considered when I try to match buyers and sellers of "
                                        "graduation tickets. Feel free to sign up again at any time.")
        subject = "Successfully Deleted Record"
        self.r.send_message(user_name, subject, message)

    def _send_missing_ceremony_message(self, err):
        message = self.reddit_paragraph("Oh no! You want to {} {} tickets, but you didn't specify which ceremony you "
                                        "wish to attend. Please send another command and include the ceremony date. "
                                        "Your choices are:".format(err.operation, err.amount))
        message += self.reddit_list(self.ceremony_dict.keys())
        message += '\n\n'
        message += self.reddit_paragraph("I recommend copying and pasting one of those dates when you write your new "
                                         "command so that I don't get confused and put the wrong date by your name in "
                                         "my database. I'm usually pretty good about parsing dates, but better safe "
                                         "than sorry!")
        subject = "Missing Ceremony in Command"
        self.r.send_message(err.user, subject, message)

    def _generate_buy_sell_notification(self, operation):

        message_template = self.reddit_paragraph("Good news! I found some students who are trying to {other_operation} "
                                                 "graduation tickets. Now you should visit their profiles and send "
                                                 "them private messages to discuss {operation}ing the tickets.")
        message_template += self.reddit_paragraph("If you end up {operation}ing tickets from anyone, please let me "
                                                  "know! Here's how you \"resolve\" a purchase:")

        message_template += self.reddit_code_block("!Faubot resolve <number> <{other_operation}er_username>\n")

        message_template += self.reddit_paragraph("For example, `!FAUbot resolve 5 /u/jpfau` means you "
                                                  "{past_tense_operation} 5 tickets from the user jpfau. For now, I "
                                                  "will only accept resolve commands from buyers, and I will ignore "
                                                  "them from sellers. It's up to you to help keep my ticket system "
                                                  "working!")
        message_template += self.reddit_paragraph("Anyway, here is the list of {other_operation}ers:")
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
            user_list = ["**/u/{}** is {}ing **{}** tickets"
                         .format(other_user['user_name'], other_user['operation'], other_user['amount'])
                         for other_user in list_users]
            message += self.reddit_list(user_list)
            subject = "It's a match!"
            self.r.send_message(user_name, subject, message)

    def notify_buyers(self, buyers):
        self._notify_buyers_sellers(buyers, 'buy')

    def notify_sellers(self, sellers):
        self._notify_buyers_sellers(sellers, 'sell')

    def megathread_missing_ceremony_reply(self, comment, err):
        user, operation, amount, choices = err.user, err.operation, err.amount, err.choices
        message = self.reddit_paragraph("Oh no! You tried to {operation} {amount} tickets, but you did not specify a "
                                        "ceremony date. Try your previous command again and include one of the "
                                        "following dates:".format(operation=operation, amount=amount))
        message += self.reddit_list(choices)
        comment.reply(message)

    def megathread_invalid_ceremony_reply(self, comment, err):
        user, operation, amount, given_date, choices = err.user, err.operation, err.amount, err.given_date, err.choices
        message = self.reddit_paragraph("Oh no! You tried to {operation} {amount} tickets, but the ceremony date "
                                        "you specified is invalid. The date you provided is:"
                                        .format(operation=operation, amount=amount))
        message += self.reddit_paragraph("{given_date}".format(given_date=given_date))
        message += self.reddit_paragraph("Please retry your previous command with one of the following dates.")
        message += self.reddit_list(choices)
        comment.reply(message)

    def megathread_invalid_command_reply(self, comment, err):
        user, bad_command = err.user, err.message_body
        message = self.reddit_paragraph("Oh no! You summoned me, but I can't make any sense of your command. You said:")
        message += self.reddit_block_quote(bad_command)
        message += self.reddit_paragraph("If you want to buy or sell tickets, please review the instructions and try "
                                         "again.")
        comment.reply(message)

    def megathread_new_user_reply(self, comment, new_user_tuple):
        """
        :type new_user_tuple: UserTuple
        :param comment:
        :param new_user_tuple:
        :return:
        """
        message = self.reddit_paragraph("Congratulations! You successfully added yourself to my database. You can see "
                                        "what your database record looks like in the Graduation Ticket Megathread in "
                                        "/r/FAU.")

        if new_user_tuple.operation == 'buy':
            message += self.reddit_paragraph("If you end up buying tickets from anyone, please let me know with a "
                                             "\"resolve\" command. You can find instructions for sending me resolve "
                                             "commands in the megathread.")
        comment.reply(message)

    def megathread_comment_resolve_reply(self, comment, resolve_tuple):
        message = self.reddit_paragraph("Congratulations! You just resolved a transaction. According "
                                        "to your command, you resolved {amount} tickets with {resolve_with}."
                                        .format(amount=resolve_tuple.amount, resolve_with=resolve_tuple.resolve_with))
        message += self.reddit_paragraph("If this is wrong, I currently don't have a way for you to fix this except "
                                         "for contacting my creator, /u/jpfau, and asking him for help.")
        comment.reply(message)

    def megathread_delete_user_reply(self, message_object):
        body = self.reddit_paragraph("You have successfully deleted yourself. You will no longer receive messages from "
                                     "me, and your name won't appear in the megathread.")
        body += self.reddit_paragraph("^^^^^you're ^^^^^dead ^^^^^to ^^^^^me ^^^^^:(")
        message_object.reply(body)

    def megathread_resolve_user_reply(self, message_object, command_tuple):
        body = self.reddit_paragraph("You have successfully resolved a transaction with /u/{}")

    def generate_megathread_body(self):
        body = self.reddit_paragraph("Hello! I am FAUbot, and welcome to the Graduation Ticket Megathread! I have "
                                     "created this post to help people buy and sell graduation tickets to each other. "
                                     "If you already know the drill, you may see the current lists of buyers and "
                                     "sellers at the end of this post, and feel free to send me new commands. If you "
                                     "don't know what I'm talking about, keep reading to learn how to interact with "
                                     "me.")
        body += self.reddit_header("Instructions")
        body += self.reddit_list([
            "Send me commands in any message that will reach my inbox, such as:", [
                "A private message",
                "A comment on this megathread",
                "A reply to any my existing posts or comments"
            ],
            "I will not check anywhere but my inbox, so any commands in messages or comments to other people will be "
            "ignored."
        ])

        body += self.reddit_paragraph()

        body += self.reddit_header("New Users", 2)

        body += self.reddit_paragraph("To add yourself to the database, which allows me to generate the buyer/seller "
                                      "lists in this thread, send me a \"New User\" command.")
        body += self.reddit_list(["Format: " + self.reddit_code_block("!FAUbot [buy|sell] [amount] [date]"),
                                  "Example: " + self.reddit_code_block("!FAUbot buy 5 December 16, 2016")])

        body += self.reddit_header("Resolving Transactions", 2)

        body += self.reddit_paragraph("If you find another Redditor who wants to buy or sell tickets from you, you "
                                      "can update both your current buy/sell amounts by sending a \"Resolve\" command.")

        body += self.reddit_list(["Format: " + self.reddit_code_block("!FAUbot resolve [amount] [other_redditor] [ceremony_date]"),
                                  "Example: " + self.reddit_code_block("!FAUbot resolve 2 /u/jpfau December 16, 2016")])

        body += self.reddit_paragraph("Please make sure only one person sends me a resolve command. If you both send "
                                      "it, I will count it twice (I am still in beta and this is a known bug).")

        body += self.reddit_header("Deleting Users", 2)
        body += self.reddit_paragraph("To delete yourself, send the delete command:")
        body += self.reddit_paragraph(self.reddit_code_block("!FAUbot delete me"))

        body += self.reddit_header("Notes", 2)
        body += self.reddit_list([
            "This tool is still in beta and may have some weird bugs if you do certain unexpected things, like:", [
                'create an excessive amount of records',
                'spam me with a bunch of requests at once (please don\'t)',
                'send me false commands, like saying you bought or sold tickets when you didn\'t'
            ],
            "I don't store any data about you besides what you give me in a command. Once you find a user who you can "
            "do business with, it's up to you to arrange things like ticket price and meeting place. ",

            "I do not guarantee anything about the business you do with other people. Using this tool means you do not "
            "hold me liable for anything that happens, like if someone rips you off."
        ])


        body += self.reddit_horizontal_rule()

        buyer_dict = self.get_buyers_for_notification_new()
        seller_dict = self.get_sellers_for_notification_new()

        table_header = ('Username', 'Ceremony Date', 'Amount')

        body += self.reddit_header("Buyers")
        if buyer_dict:
            buyer_tuples = []
            for buyer_list in buyer_dict.values():
                buyer_tuples += [("/u/{}".format(b['user_name']), b['ceremony_date'], b['amount']) for b in buyer_list]
            if buyer_tuples:
                buyer_tuples = sorted(buyer_tuples, key=lambda b: (b[1], int(b[2])), reverse=True)
                body += self.reddit_table(buyer_tuples, header=table_header)
        else:
            body += self.reddit_paragraph("There are no buyers at this time. Check again soon!")

        body += self.reddit_header("Sellers")
        if seller_dict:
            seller_tuples = []
            for seller_list in seller_dict.values():
                seller_tuples += [("/u/{}".format(s['user_name']), s['ceremony_date'], s['amount']) for s in seller_list]
            if seller_tuples:
                seller_tuples = sorted(seller_tuples, key=lambda s: (s[1], int(s[2])), reverse=True)
                body += self.reddit_table(seller_tuples, header=table_header)
        else:
            body += self.reddit_paragraph("There are no sellers at this time. Check again soon!")
        return body
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
        return TicketBot.UserTuple(user=user, operation=operation, amount=amount, date=date)

    def _parse_resolve_command(self, command, message):
        groups = command.groups()
        resolved_amount = groups[0]
        try:
            resolved_with = groups[1]
        except IndexError:
            resolved_with = None
        resolve_date = groups[2]
        try:
            resolve_date = next(d for d in self.ceremony_dict if parse(d) == parse(resolve_date))
        except StopIteration:
            raise InvalidCeremonyDate
        return TicketBot.ResolveTuple(user=message.author.name, resolve_with=resolved_with, resolve_amount=resolved_amount, date=resolve_date)

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

    def parse_commands_from_inbox(self):
        inbox = self.r.get_unread(unset_has_mail=True)
        for message in inbox:
            mark_as_read = True
            if self.DELETE_COMMAND in message.body:
                user = message.author.name.lower()
                self._new_user_delete_queue(user)
                self.megathread_delete_user_reply(message)
            else:
                try:
                    command = self.parse_command(message)
                except NoCommandInMessage:
                    mark_as_read = False
                except InvalidCommand as err:
                    self.megathread_invalid_command_reply(message, err)
                except InvalidCeremonyDate as err:
                    self.megathread_invalid_ceremony_reply(message, err)
                except MissingCeremonyDate as err:
                    self.megathread_missing_ceremony_reply(message, err)
                else:
                    if isinstance(command, TicketBot.UserTuple):
                        self._new_user_add_queue(command)
                        self.megathread_new_user_reply(message, command)
                    if isinstance(command, TicketBot.ResolveTuple):
                        self._resolve_user(command)
                        self._send_resolve_confirmation_message(command)  # todo refactor
            if mark_as_read:
                message.mark_as_read()
        self._batch_write_new_db_records_new()
    #endregion

    def match_ticket_users(self):
        users = self.get_users_for_notification_new()
        for operation_dict in users.values():
            buyers, sellers = operation_dict['buy'], operation_dict['sell']
            buyers_to_notify = {buyer['user_name']: sorted(sellers, key=lambda s: s['amount']) for buyer in buyers}
            sellers_to_notify = {seller['user_name']: sorted(buyers, key=lambda b: b['amount']) for seller in sellers}
            self.notify_buyers(buyers_to_notify)
            self.notify_sellers(sellers_to_notify)
        self._set_users_notified(users)

    def update_megathreads(self, new_body):
        for post_id in self.monitored_posts.keys():
            post = self.monitored_posts[post_id]
            if new_body != post.selftext:
                logger.info("Update megathread: subreddit=[{}], title=[{}], id=[{}]"
                            .format(post.subreddit, post.title, post_id))
                self.update_monitored_post(post_id, new_body)
            else:
                logger.info("Skip update megathread (body unchanged): subreddit=[{}], title=[{}], id=[{}]"
                            .format(post.subreddit, post.title, post_id))

    def create_megathreads(self, title, new_body):
        for subreddit in self.subreddits:

            megathread = self.get_megathread_by_title(subreddit, title=title)
            if megathread:
                self.monitored_posts[megathread.id] = megathread
                if new_body != megathread.selftext:
                    logger.info("Update megathread: subreddit=[{}], title=[{}], id=[{}]".format(subreddit, title,
                                                                                                  megathread.id))
                    megathread.edit(new_body)
                else:
                    logger.info("Skip update megathread (body unchanged): subreddit=[{}], title=[{}], "
                                "id=[{}]".format(subreddit, title, megathread.id))

            else:
                new_id = self.create_ticket_megathread(subreddit, title, new_body)
                logger.info("Create megathread: subreddit=[{}], title=[{}], id=[{}]".format(subreddit, title, new_id))

    def create_or_update_megathreads(self):
        new_megathread_body = self.generate_megathread_body()
        megathread_title = self.megathread_title()
        if self.monitored_posts:
            self.update_megathreads(new_megathread_body)
        else:
            self.create_megathreads(megathread_title, new_megathread_body)

    def work(self):
        try:
            self.refresh_ceremony_dict()
            self.parse_commands_from_inbox()
            self.create_or_update_megathreads()
        except Exception as e:
            logger.exception("Unexpected Exception: {}".format(str(e)))

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
        self.choices = choices
        msg = "Invalid Ceremony Date: given=[{}], choices={}".format(self.given_date, choices)
        super(InvalidCeremonyDate, self).__init__(msg)


class MissingCeremonyDate(ValueError):
    def __init__(self, user, operation, amount, choices):
        self.user = user
        self.amount = amount
        self.operation = operation
        self.choices = choices
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
    parser.add_argument('--loop', dest="loop", default=False, action="store_true",
                        help="Allow bot to loop instead of running once")
    args = parser.parse_args()
    test = TicketBot(args.reddit_user, run_once=not args.loop)
    test.start()
    test.stop_event.wait()


if __name__ == '__main__':
    main()
# endregion
