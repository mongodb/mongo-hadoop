require 'mongo-hadoop/input'
require 'mongo-hadoop/output'

module MongoHadoop
  def reduce
    input = BSONInput.new
    output = BSONOutput.new
    
    grouped = input.group_by { |doc| doc['_id'] }

    grouped.each do |key, values|
      output.write yield key, values
    end
  end

  def kvreduce
    kvinput = BSONKeyValueInput.new
    kvoutput = BSONKeyValueOutput.new

    grouped = kvinput.inject(Hash.new) do |hash, pair|
      key, value = *pair
      hash[key] ||= []
      hash[key] << value
      hash
    end

    grouped.each do |key, values|
      kvoutput.write yield key, values
    end
  end
end
