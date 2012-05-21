class MongoHadoop
  def self.map
    input = BSONInput.new
    output = BSONOutput.new

    input.each do |doc|
      mapped = yield doc
      mapped = [mapped] unless mapped.respond_to?(:each)

      mapped.each do |mapped|
        output.write mapped if mapped
      end
    end
  end

  def self.kvmap
    kvinput = BSONKeyValueInput.new
    kvoutput = BSONKeyValueOutput.new

    kvinput.each do |key, value|
      mapped = yield key, value
      mapped = [mapped] unless profiles.respond_to(:each)

      mapped.each do |mapped|
        kvoutput.write mapped if mapped
      end
    end
  end
end
