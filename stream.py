import json 
import tweepy
import socket
import geocoder


ACCESS_TOKEN = '2257528002-AltMHJMNLgTBmpJUC5HfiheAMMx4sEHH9KG92cQ'.strip()
ACCESS_SECRET = '	MPnCnNsxp9SZyIVRWQiqgH7AYuKEhkNsFbzfQ0LswfSkk'.strip()
CONSUMER_KEY = 'To7xmlUihF1k57t3LAUSELs5f'.strip()
CONSUMER_SECRET = 'q3oEmsAyFCjF5enLjX9aJXiqRIXqWYkAHhqFfJkI02BabsCsiS'.strip()

auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)


hashtag = '#guncontrolnow'

TCP_IP = 'localhost'
TCP_PORT = 9001


# create sockets
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
# s.connect((TCP_IP, TCP_PORT))
s.bind((TCP_IP, TCP_PORT))
s.listen(1)
conn, addr = s.accept()


class MyStreamListener(tweepy.StreamListener):
   
    def on_status(self, status):
#        print(status.text)
#        print(status.user.location)
        loc = str(status.user.location)
        print(loc)
        if( (loc != None) and (loc != "null") ):
            print("hihihihihih")
            geocode_result = geocoder.google(loc)
            lat_lng = geocode_result.latlng
            print(lat_lng)
            if(  (lat_lng != None) and ( len(lat_lng)!= 0) ):
                lat = lat_lng[0]
                lng = lat_lng[1]
            else:
                lat = 0
                lng = 0
        else:
            print("else")
            lat = 0
            lng = 0
        dict1 = {'text':status.text,'location':loc,'coordinates':[lng,lat]}
        str1 = json.dumps(dict1)
#        print(str1)

        print(type(str1))
        
        str1 = str1 + "\n"
        print(str1)
        conn.send(str1.encode("utf-8"))
    
    def on_error(self, status_code):
        if status_code == 420:
            return False
        else:
            print(status_code)

myStream = tweepy.Stream(auth=auth, listener=MyStreamListener())

myStream.filter(track=[hashtag])


