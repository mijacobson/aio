import json,couchdb

def test():

    couch = couchdb.Server()
    aiodb = couch['aio_user']
    #    print "\nCreate database 'mydb':"
    #    foo.createDb('mydb')

    #    print "\nList databases on server:"
    #    aiodb.listDb()

    print "\nCreate a document 'hkey' in database 'aiodb':"
    doc = '{"ttdid":"ABCDEFG","tmxid":"MNXPIOS","pmid":"ZASDGHJ"}'
    aiodb.save({"ttdid":"ABCDEFG","tmxid":"MNXPIOS","pmid":"ZASDGHJ"})

#    print "\nCreate a document, using an assigned docId:"
#    foo.saveDoc('mydb', doc)

#    print "\nList all documents in database 'mydb'"
#    foo.listDoc('mydb')

#    print "\nRetrieve document 'mydoc' in database 'mydb':"
#    foo.openDoc('mydb', 'mydoc')

#    print "\nDelete document 'mydoc' in database 'mydb':"
#    foo.deleteDoc('mydb', 'mydoc')

#    print "\nList all documents in database 'mydb'"
#    foo.listDoc('mydb')

#    print "\nList info about database 'mydb':"
#    foo.infoDb('mydb')

#    print "\nDelete database 'mydb':"
#    foo.deleteDb('mydb')

#    print "\nList databases on server:"
#    foo.listDb()

if __name__ == "__main__":
    test()
