# NOT DEFAULT
# Define your contracts here
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/contracts.html

from scrapy import contracts


class PostEncodedContract(contracts.Contract):
    """ Contract to change the request to a post and set the body of the request.
        The value should be a string like "GET" or "POST"
    """

    name = 'postEncoded'

    def adjust_request_args(self, args):
        args['method'] = 'POST'
        args['headers'] = {
            'Content-Type': 'application/x-www-form-urlencoded',
        }
        args['body'] = ' '.join(self.args).encode('utf-8')
        return args
