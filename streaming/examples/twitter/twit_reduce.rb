#!/usr/bin/env ruby
require 'mongo-hadoop'

# Function that takes key and array of values, iterates over all of the values,
# and returns a single document with the reduced data (summary) for that key.

MongoHadoop.reduce do |key, values|
  count = 0

  values.each do |value|
    count += value['count']
  end
  
  { :_id => key, :count => count }
end
