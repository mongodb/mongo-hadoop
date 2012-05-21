#!/usr/bin/env ruby
require 'mongo-hadoop'

MongoHadoop.kvmap do |key, value|
  [key.year, value['bc10Year']]
end
