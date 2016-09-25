import bots


class RedditPosterMixin():

    def __init__(self):
        pass

    def _r_table_divider(self, length, divider=None):
        divider = divider or "|"
        return divider.join("---" for _ in range(length)) + "\n"

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
        return table

    def reddit_paragraph(self, content):
        multipliers = tuple(abs(z) for z in range(2, -3, -1))
        for i, e in enumerate(multipliers):
            if content.endswith("\n" * e):
                return content + '\n' * multipliers[i+2]

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

    def reddit_code_block(self, content):
        content = content.strip()
        if '\n' in content:
            block = '\n'.join(('    ' + line) for line in content.split('\n'))
            return self.reddit_paragraph(block)
        else:
            return "`{}`".format(content)

if __name__ == '__main__':
    p = RedditPosterMixin()
