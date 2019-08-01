from cassandra.cluster import Cluster
import csv

class Connect(Cluster):
    
    def __init__(self):
        super().__init__()
        self.session = self.connect()
                      
        try:
            self.session.execute("""
            CREATE KEYSPACE IF NOT EXISTS test
            WITH REPLICATION = 
            { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }"""
            )
        except:
            print("Could not create the keyspace.")

        try:
            self.session.set_keyspace('test')
        except:
            print("Could not set the keyspace.")
            
    def song_duration(self):
        
        query = "CREATE TABLE IF NOT EXISTS song_duration "
        query = query + "(sessionId int, itemInSession int, artist text, song text, length float, PRIMARY KEY(sessionId, itemInSession))"
        try:
            self.session.execute(query)
        except:
            print("Could not create song_duration.")

        file = 'event_datafile_new.csv'

        with open(file, encoding = 'utf8') as f:
            csvreader = csv.reader(f)
            next(csvreader) 
            for line in csvreader:
                query = "INSERT INTO song_duration (sessionId, itemInSession, artist, song, length)"
                query = query + " VALUES (%s, %s, %s, %s, %s)"
                self.session.execute(query, (int(line[8]), int(line[3]), line[0], line[9], float(line[5])))
        
        print("song_duration was created")
        
    def song_playlist_session(self):
        
        query = "CREATE TABLE IF NOT EXISTS song_playlist_session "
        query = query + "(userId int, sessionId int, iteminSession int, artist text, song text, firstName text, lastName text, PRIMARY KEY(userId, sessionId, itemInSession))"
        try:
            self.session.execute(query)
        except:
            print("Could not create song_playlist_session.")

        file = 'event_datafile_new.csv'

        with open(file, encoding = 'utf8') as f:
            csvreader = csv.reader(f)
            next(csvreader) 
            for line in csvreader:
                query = "INSERT INTO song_playlist_session (userId, sessionId, iteminSession, artist, song, firstName, lastName)"
                query = query + " VALUES (%s, %s, %s, %s, %s, %s, %s)"
                self.session.execute(query, (int(line[10]), int(line[8]), int(line[3]), line[0], line[9], line[1], line[4]))
        
        print("song_playlist was created")
        
    def users_playlist(self):
        
        query = "CREATE TABLE IF NOT EXISTS users_playlist "
        query = query + "(song text, user_Id int, firstName text, lastName text, PRIMARY KEY(song, user_id))"
        
        try:
            self.session.execute(query)
        except:
            print("Could not create users_playlist.")

        file = 'event_datafile_new.csv'

        with open(file, encoding = 'utf8') as f:
            csvreader = csv.reader(f)
            next(csvreader) 
            for line in csvreader:
                query = "INSERT INTO users_playlist (song, user_Id, firstName, lastName)"
                query = query + " VALUES (%s, %s, %s, %s)"
                self.session.execute(query, (line[9], int(line[10]), line[1], line[4]))
                
        print("users_playlist was created")
                


def main():
    
    db = Connect()
    db.song_duration()
    db.song_playlist_session()
    db.users_playlist()
    db.session.shutdown()
    db.shutdown()
    
if __name__ == "__main__":
    main()
