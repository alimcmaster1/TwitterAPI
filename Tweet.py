class Tweet():

    def __init__(self, uniqueId, creationtimestamp, text, place, popularity):
        self.creationtimestamp = creationtimestamp
        self.uniqueId = uniqueId
        self.text = text
        self.place = place
        self.popularity = popularity

    def is_tweet_popular(self):
        return self.popularity > 0

    def object_decoder(obj):
        return Tweet(obj.get('uniqueId'), obj.get('creationtimestamp'),
                     obj.get('text'), obj.get('place'), obj.get('popularity'))

    def is_data_rich(self):
        for item in self.__dict__:
            if Tweet.check_is__valid is False:
                return False
        return True

    @staticmethod
    def check_is__valid(*args):
        for arg in args:
            if arg is None:
                return False
        return True

