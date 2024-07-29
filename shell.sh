docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' cassandra

# check mongo
docker exec -it mongo mongosh
use admin
db.auth("root","root")
use email_database
show collections
db.email_collection.findOne({"email": "sample_email@my_email.com", "otp": "1234567"})




