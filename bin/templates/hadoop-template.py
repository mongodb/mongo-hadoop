#! @PYTHON_BIN@

import sys
from os import popen
import os.path
import subprocess
import time

def stop_service(signal, service, name):
    for process in (subprocess.check_output(['jps'])).splitlines():
        if process.endswith(service):
            print("Shutting down {name}".format(**locals()))
            if sys.platform.startswith('win'):
                if signal == 'TERM':
                    subprocess.check_call(["taskkill", "/PID", process.split()[0]])
                else:
                    subprocess.check_call(["taskkill", "/F", "/PID", process.split()[0]])
            else:
                if signal == 'TERM':
                    subprocess.check_call(["kill", process.split()[0]])
                else:
                    subprocess.check_call(["kill", "-9", process.split()[0]])


def start_service(cmd, service):
    print("Starting {service}".format(**locals()))

    outfile = open("@PROJECT_HOME@/build/logs/{service}.log".format(**locals()), mode='w+')
    subprocess.Popen(["@HADOOP_HOME@/bin/{cmd}".format(**locals()), service], stdout=outfile, stderr=subprocess.STDOUT)


def start():
    shutdown()

    start_service('@BIN@', 'namenode')
    time.sleep(5)
    start_service('@BIN@', 'datanode')
    start_service('yarn', 'resourcemanager')
    start_service('yarn', 'nodemanager')
    time.sleep(15)

    print('Starting hiveserver') 
    subprocess.check_call(["@HADOOP_HOME@/bin/hadoop", "fs", "-mkdir", "-p", "hdfs:///user/hive/warehouse"])
    subprocess.check_call(["@HADOOP_HOME@/bin/hadoop", "fs", "-chmod", "g+w", "hdfs:///user/hive/warehouse"])

    # os.environ['HADOOP_HOME']='@HADOOP_HOME@'

    outfile = open("@PROJECT_HOME@/build/logs/hiveserver.log".format(**locals()), mode='w+')
    subprocess.Popen(["@HIVE_HOME@/bin/hive", "--service", "hiveserver"], stdout=outfile, stderr=subprocess.STDOUT)


def stop_all(signal):
    stop_service(signal, 'NodeManager', 'node manager')
    stop_service(signal, 'ResourceManager', 'resource manager')
    stop_service(signal, 'DataNode', 'data node')
    stop_service(signal, 'NameNode', 'name node')
    stop_service(signal, 'RunJar', 'hive server')


def shutdown():
    stop_all('TERM')
    stop_all('KILL')

def delete(path):
    if os.path.exists(path):
        for root, dirs, files in os.walk(path, topdown=False):
            for name in files:
                os.remove(os.path.join(root, name))
            for name in dirs:
                os.rmdir(os.path.join(root, name))
        os.rmdir(path)

if __name__ == "__main__":

    # os.environ["HADOOP_PREFIX"] = ""

    if not os.path.isdir("@PROJECT_HOME@/build/logs/"):
        os.mkdir('@PROJECT_HOME@/build/logs/')

    args = sys.argv[1:]
    if len(args) == 0:
        start()
    else:
        for arg in args:
            if arg == '-format':
                shutdown()
                delete("@HADOOP_BINARIES@/tmpdir/")
                out = open("@PROJECT_HOME@/build/logs/namenode-format.out", mode='w+')
                subprocess.check_call(["@HADOOP_HOME@/bin/@BIN@", 'namenode', '-format'], stdout=out, stderr=subprocess.STDOUT)
                args.remove(arg)

        if len(args) == 0:
            start()
        else:
            for arg in args:
                if arg == 'shutdown':
                    shutdown()
                elif arg == 'start':
                    start()