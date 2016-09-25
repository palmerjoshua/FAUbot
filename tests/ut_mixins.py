import ddt
from ddt import ddt, unpack, data
import unittest
from mixins import RedditPosterMixin

@ddt
class TestRedditPosterMixin(unittest.TestCase):
    test_table1 = {'header': ('title', 'description', 'foo'),
                 'rows': [('kill bill', 'action and gore', 'bar1'),
                          ('toy story', 'good cartoon', 'bar2')],
                 'output': """title | description | foo
---|---|---
kill bill | action and gore | bar1
toy story | good cartoon | bar2
"""
                   }
    test_table2 = {'header': (),
                  'rows': [test_table1['header']] + test_table1['rows'],
                  'output': test_table1['output']}

    def setUp(self):
        self.p = RedditPosterMixin()

    def tearDown(self):
        self.p = None

    @data(('foo', 'example.com', '[foo](example.com)'))
    @unpack
    def test_reddit_link(self, text, url, expected):
        link = self.p.reddit_link(text, url)
        self.assertEqual(link, expected, msg="EXPECTED: {}\tGENERATED: {}\n".format(expected, link))

    @data((('foo', 'bar'), 'foo | bar\n'),
          (('foo1', 'foo2', 'foo3'), 'foo1 | foo2 | foo3\n'))
    @unpack
    def test_reddit_table_row(self, row_tuple, expected):
        row = self.p.reddit_table_row(*row_tuple)
        self.assertEqual(row, expected, msg="EXPECTED: {}\n\nGENERATED: {}\n".format(expected, row))

    @data((test_table1['header'], test_table1['rows'], test_table1['output']),
          (test_table2['header'], test_table2['rows'], test_table2['output']))
    @unpack
    def test_reddit_table(self, header, rows, expected_output):
        table = self.p.reddit_table(rows, header=header)
        self.assertEqual(table, expected_output, msg="EXPECTED: \n{}\n\nGENERATED: \n{}".format(expected_output, table))

    @data(("foo", "foo\n\n"),
          ("foo\n", "foo\n\n"),
          ("foo\n\n", "foo\n\n"),
          ("foo\n\n\n", "foo\n\n\n"),
          ("foo\n\n\n\n", "foo\n\n\n\n"))
    @unpack
    def test_reddit_paragraph(self, content, expected):
        par = self.p.reddit_paragraph(content)
        self.assertEqual(par, expected, msg="EXPECTED:\n{}\n\nGENERATED:\n{}\n\n".format(expected, par))

    @data((['a1', 'a2', ['b1', 'b2', ['c1', 'c2'], 'b3'], 'a3'], """
* a1
* a2
 * b1
 * b2
     * c1
     * c2
 * b3
* a3

"""),
          (['a1', 'a2'], "* a1\n* a2\n\n"))
    @unpack
    def test_reddit_list(self, nested_list, expected):
        expected = expected.lstrip()
        rlist = self.p.reddit_list(nested_list)
        self.assertEqual(rlist, expected, msg="EXPECTED:\n{}\n\nGENERATED:\n{}\n\n".format(expected, rlist))

    @data(("inline code block", "`inline code block`"),
          ("inline code block\n", "`inline code block`"),
          ("multi line\ncode block", "    multi line\n    code block\n\n"),
          ("multi line\ncode block\n", "    multi line\n    code block\n\n"))
    @unpack
    def test_reddit_code(self, content, expected):
        code = self.p.reddit_code_block(content)
        self.assertEqual(code, expected, msg="\n--------\nEXPECTED:\n{}\n---------\n\n--------\nGENERATED:\n{}--------\n\n".format(expected, code))