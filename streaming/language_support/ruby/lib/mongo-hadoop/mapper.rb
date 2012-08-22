require 'mongo-hadoop/input'
require 'mongo-hadoop/output'

module MongoHadoop
  def map
    input = BSONInput.new
    output = BSONOutput.new

    input.each do |doc|
      mapped = yield doc
      mapped = [mapped] unless mapped.is_a?(Array)

      mapped.each do |mapped|
        output.write mapped if mapped
      end
    end
  end

  def kvmap
    kvinput = BSONKeyValueInput.new
    kvoutput = BSONKeyValueOutput.new

    kvinput.each do |key, value|
      mapped = yield key, value
      mapped = [mapped] unless mapped.is_a(Array)

      mapped.each do |mapped|
        kvoutput.write mapped if mapped
      end
    end
  end
end
