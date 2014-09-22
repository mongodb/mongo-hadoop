#! /usr/bin/ruby

@signal='TERM'

def stopService(service, name)
  pid = `jps | grep #{service} | cut -d' ' -f1`
  if pid != ''
    puts "Shutting down #{name}"
    %x( kill -s #{@signal} #{pid} )
  end
end

def startService(bin, service)
  puts "Starting #{service}"
  system({'HADOOP_PREFIX' => ''}, "@HADOOP_HOME@/bin/#{bin} #{service} &> '@PROJECT_HOME@/build/logs/#{service}.log' &")
end

def start()
  shutdown

  startService '@BIN@', 'namenode'
  sleep 5
  startService '@BIN@', 'datanode'
  startService 'yarn', 'resourcemanager'
  startService 'yarn', 'nodemanager'
  sleep 15

  puts 'Starting hiveserver'
  env = {}
  env['HADOOP_HOME']='@HADOOP_HOME@'
  env['HADOOP_PREFIX']=''
  system(env, "@HIVE_HOME@/bin/hadoop fs -mkdir /user/hive/warehouse")
  system(env, "@HIVE_HOME@/bin/hadoop fs -chmod g+w /user/hive/warehouse")
  
  system(env, "@HIVE_HOME@/bin/hive --service hiveserver &> '@PROJECT_HOME@/build/logs/hiveserver.log' &")
end

def stopAll()
  stopService 'NodeManager', 'node manager'
  stopService 'ResourceManager', 'resource manager'
  stopService 'DataNode', 'data node'
  stopService 'NameNode', 'name node'
  stopService 'RunJar', 'hive server'
end

def shutdown()
  stopAll
  signal='KILL'
  stopAll
end

unless File.exists?('@PROJECT_HOME@/build/logs/')
  Dir.mkdir('@PROJECT_HOME@/build/logs/')
end

if ARGV.length == 0
  start
else
  ARGV.each do |arg|
    if arg == '-format'
      shutdown
      force=''
      %x( rm -rf @HADOOP_BINARIES@/tmpdir/ )
      system({:HADOOP_PREFIX => ''}, "@HADOOP_HOME@/bin/@BIN@ namenode -format &> '@PROJECT_HOME@/build/logs/namenode-format.out'")
    end
  end

  ARGV.each do |arg|
    if arg == 'shutdown'
      shutdown
    elsif arg == 'start'
      start
    end
  end
end
