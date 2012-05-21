Gem::Specification.new do |s|
  s.name        = 'mongo-hadoop'
  s.version     = '1.0.0'
  s.date        = '2012-05-20'
  s.summary     = "MongoDB Hadoop streaming support"
  s.description = "Ruby MongoDB Hadoop streaming support"
  s.authors     = ["Tyler Brock"]
  s.email       = 'tyler.brock@gmail.com'
  s.files       = [
    "lib/mongo-hadoop.rb",
    "lib/mongo-hadoop/input.rb",
    "lib/mongo-hadoop/output.rb",
    "lib/mongo-hadoop/mapper.rb",
    "lib/mongo-hadoop/reducer.rb"
  ]
  s.homepage = 'http://github.com/mongodb/mongo-hadoop'
  s.add_dependency 'bson'
end
