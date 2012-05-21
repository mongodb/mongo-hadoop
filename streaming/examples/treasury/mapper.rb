#!/usr/bin/env ruby
require 'mongo-hadoop'

MongoHadoop.map do |document|
  { :_id => document['_id'].year, :bc10Year => document['bc10Year'] }
end
