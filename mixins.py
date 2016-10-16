from abc import abstractmethod

from praw.helpers import flatten_tree

import bots
from pprint import PrettyPrinter
p = PrettyPrinter(indent=2)

class MarkdownMaker:
    def _r_table_divider(self, length, divider="|", sep="---"):
        return divider.join(sep for _ in range(length)) + "\n"

    def reddit_table_row(self, *row_items, header=False):
        divider = "{0}|{0}"
        spaced, unspaced = divider.format(" "), divider.format("")
        row = spaced.join(item or " " for item in row_items) + "\n"
        if header:
            row += self._r_table_divider(len(row_items))
        return row

    def reddit_table_header(self, *column_names):
        return self.reddit_table_row(*column_names, header=True)

    def reddit_link(self, title, url):
        return "[{}]({})".format(title, url)

    def reddit_table(self, table_rows, header=()):
        table = ""
        if header:
            to_insert = header
            row_start_index = 0
        else:
            to_insert = table_rows[0]
            row_start_index = 1
        table += self.reddit_table_header(*to_insert)
        for row_item in table_rows[row_start_index:]:
            table += self.reddit_table_row(*row_item)
        return self.reddit_paragraph(table)

    def reddit_paragraph(self, content=""):
        new_line = "\n"
        multipliers = tuple(abs(z) for z in range(2, -3, -1))
        for i, e in enumerate(multipliers):
            if content.endswith(new_line * e):
                return content + new_line * multipliers[i+2]

    def _reddit_list(self, nested_list, current_indentation_level):
        to_return = ""
        for item in nested_list:
            if isinstance(item, list):
                new_indentation_level = 1 if current_indentation_level == 0 else current_indentation_level + 4
                to_return += self._reddit_list(item, new_indentation_level)
            else:
                to_return += (" " * current_indentation_level) + "* {}".format(item) + '\n'
        return to_return

    def reddit_list(self, nested_list):
        rlist = self._reddit_list(nested_list, 0)
        return self.reddit_paragraph(rlist)

    def reddit_newline(self):
        return "\n\n"

    def reddit_code_block(self, content):
        content = content.strip()
        if '\n' in content:
            block = '\n'.join(('    ' + line) for line in content.split('\n'))
            return self.reddit_paragraph(block)
        else:
            return "`{}`".format(content)

    def reddit_block_quote(self, content):
        content = content.strip()
        quote = self.reddit_newline().join(">"+line for line in content.split(self.reddit_newline()))
        return self.reddit_paragraph(quote)

    def reddit_header(self, header_value, header_level=1):
        header_level = header_level or 1
        header = ("#" * header_level) + header_value
        return self.reddit_paragraph(header)

    def reddit_horizontal_rule(self):
        return self.reddit_paragraph("---")


class RedditPosterMixin(bots.RedditBot, MarkdownMaker):
    def __init__(self, user_name, *args, **kwargs):
        super(RedditPosterMixin, self).__init__(user_name, *args, **kwargs)
        self.monitored_posts = dict()
        self._read_comments = dict()

    @abstractmethod
    def work(self):
        pass

    def create_monitored_post(self, subreddit, title, body):
        submission = self.r.submit(subreddit, title, body)
        self.monitored_posts[submission.id] = submission
        return submission.id

    def get_monitored_post(self, post_id, refresh=True):
        post = self.monitored_posts[post_id]
        if refresh:
            post.refresh()
        return post

    def update_monitored_post(self, post_id, new_text):
        self.monitored_posts[post_id].edit(new_text)

    def delete_monitored_post(self, post_id):
        self.monitored_posts[post_id].delete()
        del self.monitored_posts[post_id]

    def monitor(self, post_id, trigger, response, *args, **kwargs):
        post = self.get_monitored_post(post_id)
        comments = filter(lambda c: c.id not in self._read_comments.keys(), flatten_tree(post.comments))
        for comment in comments:
            if trigger in comment.body:
                response(comment, *args, **kwargs)
