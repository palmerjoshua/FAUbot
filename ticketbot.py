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

    @staticmethod
    def get_ceremony_dict():
        data = TicketBot._get_ceremony_data()
        return TicketBot._get_ceremony_dict(data)

    def get_current_season(self):
        self.ceremony_dict = TicketBot.get_ceremony_dict()
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

    def _delete_db_record(self, user_name, ceremony_date):
        logger.info("Deleting user from database: user=[{}]".format(user_name))
        ticketbot_table = self._get_ticketbot_table()
        ticketbot_table.delete_item(Key=self._get_user_key(user_name, ceremony_date))

    def _update_db_record(self, user_name, ceremony_date, operator_string, **items_to_update):
        update_expression = ",".join("SET {value} {operator} :{value}".format(value=item, operator=operator_string) for item in items_to_update)
        expression_attribute_values = {':{}'.format(item): new_value for item, new_value in items_to_update.items()}
        ticketbot_table = self._get_ticketbot_table()
        ticketbot_table.update_item(
            Key=self._get_user_key(user_name, ceremony_date),
            UpdateExpression=update_expression,
            ExpressionAttributeValues=expression_attribute_values
        )

    def _increment_db_item_value(self, user_name, value_name, delta):
        to_update = {value_name: delta}
        self._update_db_record(user_name, "= {} +".format(value_name), **to_update)

    def _decrement_db_item_value(self, user_name, value_name, delta):
        to_update = {value_name: delta}
        self._update_db_record(user_name, "= {} -".format(value_name), **to_update)

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
    def _set_user_resolved(self, resolve_tuple, _ticketbot_table=None):  # todo refactor
        """
        :type resolve_tuple: TicketBot.ResolveTuple
        :param resolve_tuple:
        :return:
        """
        user_name, resolved_with, \
        resolved_amount, ceremony_date = resolve_tuple.user.lower(), resolve_tuple.resolve_with.lower(), \
                                         int(resolve_tuple.resolve_amount), resolve_tuple.date

        logger.info("Resolving transaction between: user=[{}], resolvedWith=[{}]".format(user_name, resolved_with))
        logger.info("Updating user in database: user=[{}]".format(user_name))
        self._change_user_ticket_amount(user_name, -resolved_amount)
        logger.info("Updating user in database: user=[{}]".format(resolved_with))
        self._change_user_ticket_amount(resolved_with, -resolved_amount)

    def _set_user_ticket_amount(self, user_name, new_amount):
        logger.info("Updating ticket amount in database: user=[{}], newAmount=[{}]".format(user_name, new_amount))
        self._update_db_record(user_name, "=", amount=new_amount)

    def _change_user_ticket_amount(self, user_name, delta):
        self._increment_db_item_value(user_name, 'amount', delta)
    # endregion

    # region Get-Users
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
        buyers = {ceremony: self._get_buyers_by_date(ceremony) for ceremony in self.ceremony_dict}
        to_delete = [key for key, val in buyers.items() if not val]
        for key in to_delete:
            del buyers[key]
        return buyers

    def get_sellers_for_notification(self):
        sellers = {ceremony: self._get_sellers_by_date(ceremony) for ceremony in self.ceremony_dict}
        to_delete = [key for key, val in sellers.items() if not val]
        for key in to_delete:
            del sellers[key]
        return sellers

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
    def _send_invalid_ceremony_date_message(self, err):
        logger.info("Sending invalid ceremony date message: user=[{}], givenDate=[{}]".format(err.user, err.given_date))
        message = "Uh oh! Your last command included a date on which there are no ceremonies scheduled.\n\n" \
                  "Your last command was: `{} {} {}`\n\n" \
                  "Here are your choices:\n\n".format(err.operation, err.amount, err.given_date)
        message += self.reddit_list(self.ceremony_dict.keys())
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
        message = self.reddit_paragraph("Congratulations! You successfully added yourself to my database. I will try "
                                        "to match you with people who can help you {operation} the tickets you need. "
                                        "I will PM you once per day with a list of matches. You can also check this "
                                        "megathread, as I will be updating the lists of buyers and sellers throughout "
                                        "the day.".format(operation=new_user_tuple.operation))
        if new_user_tuple.operation == 'buy':
            message += self.reddit_paragraph("If you end up buying tickets from anyone, please let me know with a "
                                             "\"resolve\" command. You can find instructions for sending me resolve "
                                             "commands in the body of this megathread.")
        comment.reply(message)

    def megathread_comment_resolve_reply(self, comment, resolve_tuple):
        message = self.reddit_paragraph("Congratulations! You just saved your transaction to my database. According "
                                        "to your command, you resolved {amount} tickets with {resolve_with}."
                                        .format(amount=resolve_tuple.amount, resolve_with=resolve_tuple.resolve_with))
        message += self.reddit_paragraph("If this is wrong, I currently don't have a way for you to fix this except "
                                         "for contacting my maker, /u/jpfau, and asking him for help.")
        comment.reply(message)

    def generate_megathread_body(self):
        table_header = ('Username', 'Amount', 'Ceremony Date')

        body = self.reddit_header("Instructions")
        body += self.reddit_paragraph("*Note: All commands can be sent to me in a PM, or you can send a command as a "
                                      "comment in this megathread.*")

        body += self.reddit_header("New Users", 2)
        body += self.reddit_paragraph("To add yourself to the database, which allows me to match you with other "
                                         "users and generate the buyer/seller lists in this thread, send me a "
                                         "\"New User\" command.")
        body += self.reddit_list(["Format: " + self.reddit_code_block("!FAUbot [buy|sell] [amount] [date]"),
                                  "Example: " + self.reddit_code_block("!FAUbot buy 5 December 16, 2016")])

        body += self.reddit_header("Resolving Transactions", 2)
        body += self.reddit_paragraph("If you find another Redditor who wants to buy or sell tickets from you, you "
                                      "can update both your current buy/sell amounts by sending a \"Resolve\" command.")
        body += self.reddit_list(["Format: " + self.reddit_code_block("!FAUbot resolve [amount] [other_redditor] [ceremony_date]"),
                                  "Example: " + self.reddit_code_block("!FAUbot resolve 2 /u/jpfau December 16, 2016")])
        body += self.reddit_paragraph("*Note: I currently accept resolve commands from __buyers only__. If you "
                                      "just sold some tickets and want to update your current ticket amount, make sure"
                                      "the buyer sends me a resolve command.*")
        body += self.reddit_horizontal_rule()


        buyer_dict = self.get_buyers_for_notification()
        seller_dict = self.get_sellers_for_notification()

        body += self.reddit_header("Buyers")
        for buyer_list in buyer_dict.values():
            buyer_tuples = [(b['user_name'], b['amount'], b['ceremony_date']) for b in buyer_list]
            if buyer_tuples:
                body += self.reddit_table(buyer_tuples, header=table_header)

        body += self.reddit_header("Sellers")
        for seller_list in seller_dict.values():
            seller_tuples = [(s['user_name'], s['amount'], s['ceremony_date']) for s in seller_list]
            if buyer_tuples:
                body += self.reddit_table(seller_tuples, header=table_header)
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

    @staticmethod
    def _parse_resolve_command(command, message):
        groups = command.groups()
        resolved_amount = groups[0]
        try:
            resolved_with = groups[1]
        except IndexError:
            resolved_with = None
        resolve_date = groups[2]
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

    def monitor_comment_ticketbot(self, comment):
        comment.refresh()
        try:
            command = self.parse_command(comment)
        except NoCommandInMessage:
            return
        except InvalidCommand as err:
            self.megathread_invalid_command_reply(comment, err)
        except InvalidCeremonyDate as err:
            self.megathread_invalid_ceremony_reply(comment, err)
        except MissingCeremonyDate as err:
            self.megathread_missing_ceremony_reply(comment, err)
        else:
            self._read_comments[comment.id] = comment
            command_function = self.get_command_function(command, self.megathread_new_user_reply,
                                                         self.megathread_comment_resolve_reply)
            command_function(comment, command)

    def get_command_function(self, command, commandtuple_fn, resolvetuple_fn):
        if isinstance(command, TicketBot.UserTuple):
            self._new_user_add_queue(command)
            return commandtuple_fn
        if isinstance(command, TicketBot.ResolveTuple):
            self._set_user_resolved(command)
            return resolvetuple_fn

    def parse_commands_from_inbox(self):
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
                    function, params = self.get_command_function(command, self._send_confirmation_message,
                                                                 self._send_resolve_confirmation_message), command
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

    def parse_commands_from_megathread(self):
        if self.monitored_posts:
            logger.info("Parsing commands from megathreads")
            for thread_id in self.monitored_posts.keys():
                self.monitor(thread_id, '!FAUbot', self.monitor_comment_ticketbot)
        else:
            logger.info("Skipping parse of megathread commands because none exist yet.")

    def update_megathreads(self, new_body):
        for post_id in self.monitored_posts.keys():
            post = self.monitored_posts[post_id]
            if new_body != post.selftext:
                logger.info("Updating megathread: subreddit=[{}], title=[{}], id=[{}]"
                            .format(post.subreddit, post.title, post_id))
                self.update_monitored_post(post_id, new_body)
            else:
                logger.info("Megathread selftext unchanged. Skipping update: subreddit=[{}], title=[{}], id=[{}]"
                            .format(post.subreddit, post.title, post_id))

    def create_or_update_megathreads(self):
        new_megathread_body = self.generate_megathread_body()
        megathread_title = self.megathread_title()
        if not self.monitored_posts:
            for subreddit in self.subreddits:

                megathread = self.get_megathread_by_title(subreddit, title=megathread_title)
                if megathread:
                    self.monitored_posts[megathread.id] = megathread
                    if new_megathread_body != megathread.selftext:
                        logger.info("Updating megathread: subreddit=[{}], title=[{}], id=[{}]".format(subreddit,
                                                                                                  megathread_title,
                                                                                                  megathread.id))
                        self.update_monitored_post(megathread.id, new_megathread_body)
                    else:
                        logger.info("Megathread selftext unchanged. Skipping update: subreddit=[{}], title=[{}], "
                                    "id=[{}]".format(subreddit, megathread_title, megathread.id))

                else:
                    new_id = self.create_ticket_megathread(subreddit, megathread_title, new_megathread_body)
                    logger.info("Creating megathread: subreddit=[{}], title=[{}], id=[{}]".format(subreddit,
                                                                                                  megathread_title,
                                                                                                  new_id))
        else:
            self.update_megathreads(new_megathread_body)

    def work(self):
        self.ceremony_dict = self.get_ceremony_dict()
        self.parse_commands_from_inbox()
        #self.parse_commands_from_megathread()
        #self.create_or_update_megathreads()
        #self.match_ticket_users()
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
