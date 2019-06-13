from selenium_client import SeleniumClient


class Comments(object):
    list_comments = None

    def __call__(self, query):
        if not Comments.list_comments:
            Comments.list_comments = SeleniumClient()
        # list_all = []
        # for query in queries:
        #     l = Comments.list_comments.get_comments(query)
        #     list_all.extend(l)
        #return list_all
        return Comments.list_comments.get_comments(query)
