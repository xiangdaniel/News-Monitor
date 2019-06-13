from selenium import webdriver
from selenium.webdriver.common.keys import Keys
import time


class SeleniumClient(object):
    def __init__(self):
        # Initialization method.
        self.chrome_options = webdriver.ChromeOptions()
        self.chrome_options.add_argument('--headless')
        self.chrome_options.add_argument('--no-sandbox')
        self.chrome_options.add_argument('--disable-setuid-sandbox')

        # you need to provide the path of chromdriver in your system
        self.browser = webdriver.Chrome('../static/chromedriver',
                                        options=self.chrome_options)  # , options=self.chrome_options

        self.base_url = 'https://twitter.com/search?q='

    # def get_all_comments(self, queries):
    #     list_all_comments = []
    #     for query in queries:
    #         list_all_comments.extend(self.get_comments(query))
    #     return list_all_comments

    def parse_count(self, like_str):
        if len(like_str) == 0:
            return 0
        if 'K' in like_str:
            return int(float(like_str.replace('K', '')) * 1e3)
        elif 'k' in like_str:
            return int(float(like_str.replace('k', '')) * 1e3)
        elif 'M' in like_str:
            return int(float(like_str.replace('M', '')) * 1e6)
        elif 'm' in like_str:
            return int(float(like_str.replace('m', '')) * 1e6)
        elif 'B' in like_str:
            return int(float(like_str.replace('B', '')) * 1e9)
        elif 'b' in like_str:
            return int(float(like_str.replace('b', '')) * 1e9)
        return int(like_str)

    def get_comments(self, query):
        '''
        Function to fetch tweets.
        '''
        try:
            self.browser.get(query) #self.base_url+
            time.sleep(2)

            comments_count = self.parse_count(self.browser.find_element_by_css_selector('.ProfileTweet-actionCountForPresentation').text)
            if comments_count == 0:
                return None
            #print(comments_count)
            n = 0
            pn = 0
            body = self.browser.find_element_by_tag_name('body')
            buppton = body.find_element_by_css_selector('.ProfileTweet-actionButton.u-textUserColorHover.dropdown-toggle.js-dropdown-toggle')
            buppton.click()
            #body.click()
            i = 0
            repeat_counts = 0
            while n < comments_count and repeat_counts < 100:
                i += 1
                descendants = self.browser.find_element_by_id('descendants')
                tweet_nodes = descendants.find_elements_by_css_selector('.js-tweet-text.tweet-text')
                comment_times = descendants.find_elements_by_css_selector('.tweet-timestamp')
                likes = descendants.find_elements_by_css_selector('.ProfileTweet-action.ProfileTweet-action--favorite')
                n = len(tweet_nodes)
                #print(n)
                if n == pn:
                    repeat_counts += 1
                else:
                    repeat_counts = 0
                body.send_keys(Keys.PAGE_DOWN)
                time.sleep(0.3)
                pn = n

            ls_comments = []
            i = 0
            # print(len(likes))
            # print(len(tweet_nodes))
            # print(len(comment_times))
            for tweet_node in tweet_nodes:
#                 if len(tweet_node.text) == 0:
#                     continue
                like_str = likes[i].find_element_by_css_selector('.ProfileTweet-actionCountForPresentation').text
                if len(like_str) == 0:
                    like_count = 1
                else:
                    like_count = 1 + self.parse_count(like_str)
                ls_comments.append(
                    (str(like_count),
                     comment_times[i].get_attribute("title"),
                     tweet_node.text)
                )
                i += 1
#             for tweet_node in tweet_nodes:
#                 ls_comments.append((tweet_node.text, comment_times.get_attribute("data-original-title")))
            return ls_comments

        except Exception as e:
            print(e)
            print("Selenium - An error occured while fetching tweets.")
