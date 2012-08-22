require 'bson'

class BSONInput
  include Enumerable
  
  def initialize(stream=nil)
    @stream = stream || $stdin
  end
  
  def read
    begin
      BSON.read_bson_document(@stream)
    rescue NoMethodError
      nil
    end
  end

  def each
    while(doc = read)
      yield doc
    end
  end
end

class BSONKeyValueInput < BSONInput
  def each
    while(doc = read)
      yield doc['_id'], doc
    end
  end
end
