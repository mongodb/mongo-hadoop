#!/usr/bin/env ruby
require 'mongo-hadoop'

MongoHadoop.kvreduce do |key, values|
  count = sum = 0

  values.each do |value|
    count += 1
    sum += value['value']
  end

  [key, sum / count]
end
