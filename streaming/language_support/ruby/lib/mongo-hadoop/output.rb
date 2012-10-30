require 'bson'

class BSONOutput
  def initialize(stream=nil)
    @stream = stream || $stdout
  end
  
  def write(doc)
    bson_doc = BSON.serialize(doc)
    @stream.write(bson_doc)
    @stream.flush
  end
end

class BSONKeyValueOutput < BSONOutput
  def write(pair)
    key, value = *pair

    doc = value.is_a?(Hash) ? value : { :value => value }

    doc['_id'] = key
    super(doc)
  end
end
