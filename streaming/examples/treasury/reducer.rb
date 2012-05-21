#!/usr/bin/env ruby
require 'mongo-hadoop'

MongoHadoop.reduce do |key, values|
  count = sum = 0
  
  values.each do |value|
    count += 1
    sum += value['bc10Year']
  end

  { :_id => key, :average => sum / count }
end
