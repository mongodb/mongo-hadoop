#!/usr/bin/env ruby
require 'mongo-hadoop'

MongoHadoop.map do |document|
  if document.has_key?('headers')
    headers = document['headers']
    if ['To', 'From'].all? { |header| headers.has_key? (header) }
      to_field = headers['To']
      from_field = headers['From']
      recipients = to_field.split(',').map { |recipient| recipient.strip }
      recipients.map { |recipient| {:_id => {:f => from_field, :t => recipient}, :count => 1} }
    end
  end
end
