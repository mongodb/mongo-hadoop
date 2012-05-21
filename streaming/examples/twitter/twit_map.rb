#!/usr/bin/env ruby
require 'mongo-hadoop'

MongoHadoop.map do |document|
  { :_id => document['user']['time_zone'], :count => 1 }
end
