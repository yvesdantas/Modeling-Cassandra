from db_connect import Connect
from cassandra.cluster import Cluster

class Drop(Cluster):
    
    def __init__(self):
        super().__init__()
        self.session = self.connect()
        
    def connect_keyspc(self, duck):
        
        duck.connect_keyspc()
        
    def drop_song_duration(self):
        
        query = "DROP TABLE IF EXISTS test.song_duration"
        
        try:
            self.session.execute(query)
            print("song_duration deleted")
        except:
            print("couldnt drop song_duration")
            
    def drop_song_playlist_session(self):
        
        query = "DROP TABLE IF EXISTS test.song_playlist_session"
        
        try:
            self.session.execute(query)
            print("song_playlist_session deleted")
        except:
            print("couldnt drop song_playlist_session")
            
    def drop_users_playlist(self):
        
        query = "DROP TABLE IF EXISTS test.users_playlist"
        
        try:
            self.session.execute(query)
            print("users_playlist deleted")
        except:
            print("couldnt drop users_playlist")
            

def main():
    
    db = Drop()
    db.drop_song_duration()
    db.drop_song_playlist_session()
    db.drop_users_playlist()
    db.session.shutdown()
    db.shutdown()
    
if __name__ == "__main__":
    main()
            
    
        
        
        