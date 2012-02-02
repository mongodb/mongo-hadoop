Importing Live Twitter Data, you'll need a twitter login and password:

    curl https://stream.twitter.com/1/statuses/sample.json -u<login>:<password> | mongoimport -d test -c live

This will continue streaming until you ^C it.
