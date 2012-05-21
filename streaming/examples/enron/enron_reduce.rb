#!/usr/bin/env ruby
require 'mongo-hadoop'

MongoHadoop.reduce do |key, values|
  count = values.reduce { |sum, current| sum += current['count'] }

  { :_id => key, :count => count }
end
