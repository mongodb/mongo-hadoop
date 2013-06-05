try:
    import json
except ImportError:
    import simplejson as json
import logging
import os
import random
import shutil
import signal
import socket
import subprocess
import sys
import time

import pymongo

hostname = "127.0.0.1"#socket.gethostname()
start_port = int(os.environ.get('DB_PORT', 17000)) 
standalone_count = 0
mongos_count = 0
mongod = os.environ.get('MONGOD', 'mongod')
mongos = os.environ.get('MONGOS', 'mongos')
mongoimport = os.environ.get('MONGOIMPORT', 'mongoimport')
mongodump = os.environ.get('MONGODUMP', 'mongodump')
mongorestore = os.environ.get('MONGORESTORE', 'mongorestore')
print "> MongoDB Path: %s " % mongod
replsets = 0

def mongo_dump(host, db, collection, outputdir):
    cmd = [mongodump, 
           '--host', str( host ), 
           '-d', db,
           '-c', collection,
           '-o', outputdir]
    subprocess.call(cmd)

def mongo_restore(host, db, collection, filename):
    cmd = [mongorestore, 
           '--host', str( host ), 
           '--db', db,
           '--collection', collection,
           filename]
    subprocess.call(cmd)

def mongo_import(host, db, collection, filename):
    cmd = [mongoimport, 
           '--host', str( host ), 
           '--db', db,
           '--collection', collection,
           '--file', filename]
    subprocess.call(cmd)

class MongosManager(object):
    def __init__(self, port=None, home=None):
        global start_port, replsets, mongos_count
        self.home = home
        if not port:
            self.port = start_port
            start_port += 1
        if not self.home:
            self.home = os.environ.get("HOME")
        self.name = "mongos" + str(mongos_count)
        mongos_count += 1
        self.logpath = os.path.join(self.home, "log_" + self.name)
        self.dbpath = os.path.join(self.home, "data_" + self.name)

    def connection(self):
        return pymongo.Connection('localhost:' + str(self.port))

    def start_mongos(self, confighost, shards, noauth=False, fresh=True, addShards=False):
        global mongos
        self.host = '%s:%d' % (hostname, self.port)
        if fresh:
            try:
                logging.info("Deleting dbpth '%s'" % self.dbpath)
                shutil.rmtree(self.dbpath)
            except OSError:
                # dbpath doesn't exist yet
                pass

            try:
                logging.info("Deleting logpath '%s'" % self.logpath)
                shutil.rmtree(self.logpath)
            except OSError:
                # logpath doesn't exist yet
                pass

        if not os.path.exists(self.home):
            os.makedirs(self.home)
            os.chdir(self.home)

        self.start_time = time.time()
        self.info = {}
        host = '%s:%d' % (hostname, self.port)
        path = os.path.join(self.dbpath, 'db' + str(self.port))
        if not os.path.exists(path):
            logging.info("Making Data directory: %s" % path)
            os.makedirs(path)

        member_logpath = os.path.join(self.logpath, 'db' + str(self.port) + '.log')
        if not os.path.exists(os.path.dirname(member_logpath)):
            os.makedirs(os.path.dirname(member_logpath))

        keyFilePath = os.path.join(path, 'keyFile')
        keyFile = open(keyFilePath, 'w+')
        keyFile.write('password1')
        keyFile.close()
        os.chmod(keyFilePath, int('0600', 8))

        cmd = [
            mongos,
            '--configdb', confighost,
            '--port', str(self.port),
            '--logpath', member_logpath,
            #'--nohttpinterface',
            '-vvvvv',
            '--ipv6',
        ]

        if not noauth:
            cmd.extend(['--keyFile', keyFilePath,])

        print "starting", cmd
        logging.info('Starting %s' % ' '.join(cmd))
        proc = subprocess.Popen(cmd,
            stderr=subprocess.STDOUT)
        res = self.wait_for(proc, self.port)
        self.pid = proc.pid
        print "pid is", self.pid

        if addShards:
            mongos_cxn = pymongo.Connection(self.host)
            admin = mongos_cxn['admin']
            for shard in shards:
                admin.command("addShard", shard)
        
        if not res:
            raise Exception("Couldn't execute %s" % ' '.join(cmd))
        else:
            return host;

    def wait_for(self, proc, port):
        trys = 0
        return_code = proc.poll()
        while return_code is None and trys < 100: # ~25 seconds
            trys += 1
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                try:
                    s.connect((hostname, port))
                    return True
                except (IOError, socket.error):
                    time.sleep(0.25)
            finally:
                s.close()
            return_code = proc.poll()

        self.kill_all_members(sig=9) #TODO
        return False

    def kill_all_members(self, sig=2):
        if self.pid is not None:
            print "killing ", self.pid
            if sys.platform in ('win32', 'cygwin'):
                os.kill(self.pid, signal.CTRL_C_EVENT)
            else:
                os.kill(self.pid, sig)

class StandaloneManager(object):
    def __init__(self, port=None, home=None):
        global start_port, replsets, standalone_count
        self.home = home
        if not port:
            self.port = start_port
            start_port += 1
        if not self.home:
            self.home = os.environ.get("HOME")
        self.name = "standalone" + str(standalone_count)
        standalone_count += 1
        self.logpath = os.path.join(self.home, "log_" + self.name)
        self.dbpath = os.path.join(self.home, "data_" + self.name)

    def connection(self):
        return pymongo.Connection('localhost:' + str(self.port))

    def wait_for(self, proc, port):
        trys = 0
        return_code = proc.poll()

        while return_code is None and trys < 100: # ~25 seconds
            trys += 1
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                try:
                    s.connect((hostname, port))
                    return True
                except (IOError, socket.error):
                    time.sleep(0.25)
            finally:
                s.close()

            return_code = proc.poll()
        self.kill_all_members(sig=9) #TODO
        return False

    def kill_all_members(self, sig=2):
        if self.pid is not None:
            print "killing ", self.pid
            if sys.platform in ('win32', 'cygwin'):
                os.kill(self.pid, signal.CTRL_C_EVENT)
            else:
                os.kill(self.pid, sig)

    def start_mongod(self, port, noauth=False):
        self.host = '%s:%d' % (hostname, port)
        path = os.path.join(self.dbpath, 'db' + str(port))
        if not os.path.exists(path):
            logging.info("Making Data directory: %s" % path)
            os.makedirs(path)

        keyFilePath = os.path.join(path, 'keyFile')
        keyFile = open(keyFilePath, 'w+')
        keyFile.write('password1')
        keyFile.close()
        os.chmod(keyFilePath, int('0600', 8))

        member_logpath = os.path.join(self.logpath, 'db' + str(port) + '.log')
        if not os.path.exists(os.path.dirname(member_logpath)):
            logging.info("Making Log directory: %s" % os.path.dirname(member_logpath))
            os.makedirs(os.path.dirname(member_logpath))
        cmd = [
            mongod,
            '--dbpath', path,
            '--port', str(port),
            '--logpath', member_logpath,
            '--nojournal',
            # Various attempts to make startup faster on Mac by limiting
            # the size of files created at startup
            '--nohttpinterface',
            '--noprealloc',
            '--smallfiles',
            '--nssize', '1',
            '-vvvvv',
            # Ensure that Mongo starts with all its features turned on so we
            # can test them!
            '--ipv6',
        ]

        if not noauth:
            cmd.extend(['--auth',
                        '--keyFile', keyFilePath,])

        logging.info('Starting %s' % ' '.join(cmd))
        proc = subprocess.Popen(cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT)
        res = self.wait_for(proc, port)
        self.pid = proc.pid
        print "pid is", self.pid
        if not res:
            raise Exception("Couldn't execute %s" % ' '.join(cmd))
        else:
            return self.host;

    def start_server(self,fresh=False):
        if fresh:
            try:
                logging.info("Deleting dbpth '%s'" % self.dbpath)
                shutil.rmtree(self.dbpath)
            except OSError:
                # dbpath doesn't exist yet
                pass

            try:
                logging.info("Deleting logpath '%s'" % self.logpath)
                shutil.rmtree(self.logpath)
            except OSError:
                # logpath doesn't exist yet
                pass

        # If we just deleted the working dir (because it was equal to, or
        # beneath, the dbpath or logpath), then recreate the working directory
        # and change to the new copy of it.
        if not os.path.exists(self.home):
            os.makedirs(self.home)
            os.chdir(self.home)

        self.start_time = time.time()
        self.info = {}
        self.host = self.start_mongod(self.port)
        return self.host

class ReplicaSetManager(object):

    def __init__(self, port=None, name=None, home=None, with_arbiter=True, num_members=3, master_slave=False, noauth=False):
        global start_port, replsets
        self.home = home
        self.num_members = num_members
        self.with_arbiter = with_arbiter
        self.master_slave = master_slave
        self.noauth = noauth
        self.members_info = {}
        
        if not name:
            self.name = "replset" + str(replsets)
            replsets += 1
        if not port:
            self.port = start_port
            start_port += num_members
        else:
            self.port = int(port)
        if not self.home:
            self.home = os.environ.get("HOME")
        self.logpath = os.path.join(self.home, "log_" + self.name)
        self.dbpath = os.path.join(self.home, "data_" + self.name)

    def get_shard_string(self): 
        return self.name + "/" + ",".join([host for host in self.members_info.keys()])

    def kill_members(self, members, sig=2):
        for member in members:
            if member not in self.members_info:
                # We killed this member earlier, but it's still in the
                # replSetGetStatus() output as 'stateStr: (not reachable/healthy)'
                continue

            try:
                pid = self.members_info[member]['pid']
                logging.info('Killing pid %s' % pid)
                # Not sure if cygwin makes sense here...
                if sys.platform in ('win32', 'cygwin'):
                    os.kill(pid, signal.CTRL_C_EVENT)
                else:
                    os.kill(pid, sig)

                # Make sure it's dead
                os.waitpid(pid, 0)
                logging.info('Killed.')
            except OSError:
                pass # already dead

            del self.members_info[member]

    def kill_all_members(self, sig=2):
        self.kill_members(self.members_info.keys(), sig=sig)

    def kill_primary(self):
        primary = self.get_primary()
        self.kill_members(primary)
        return primary

    def kill_secondary(self):
        secondary = self.get_random_secondary()
        self.kill_members(secondary)
        return secondary

    def kill_all_secondaries(self):
        secondaries = self.get_all_secondaries()
        self.kill_members(secondaries)
        return secondaries

    def get_master(self):
        return [
            name for name, info in self.members_info.items()
            if info.get('master')
        ]

    def get_slaves(self):
        return [
            name for name, info in self.members_info.items()
            if info.get('slave')
        ]

    def get_primary(self):
        return self.get_members_in_state(1)

    def get_random_secondary(self):
        secondaries = self.get_members_in_state(2)
        if len(secondaries):
            return [secondaries[random.randrange(0, len(secondaries))]]
        return secondaries

    def get_secondaries(self):
        return self.get_members_in_state(2)

    def get_arbiters(self):
        return self.get_members_in_state(7)

    def wait_for(self, proc, port):
        trys = 0
        return_code = proc.poll()
        if return_code is not None: return True
        while return_code is None and trys < 100: # ~25 seconds
            trys += 1
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                try:
                    s.connect((hostname, port))
                    return True
                except (IOError, socket.error):
                    time.sleep(0.25)
            finally:
                s.close()

            return_code = proc.poll()

        logging.error(
            "Couldn't start member on port %s, return_code = %s, killing other" \
            " members and quitting" % (port, return_code)
        )

        self.kill_all_members(sig=0) #TODO
        return False


    def start_member(
            self,
            port,
            _id,
            arbiter=False,
            master=False,
            slave=False,
            source_port=None,
            noauth=False,
            extras={}
        ):
        """
        Start a replica-set member on port.
        @param port:        Int, the port to start this member on
        @param _id:         Unique int
        @param arbiter:     Bool, whether to make this an arbiter
        @param master:      Whether this is a master in a master-slave pair
        @param slave:       Whether this is a slave in a master-slave pair
        @param source_port: If this is a slave, the master's port number
        @return:        Member info, suitable as part of replSetInitiate()
        """
        host = '%s:%d' % (hostname, port)
        path = os.path.join(self.dbpath, 'db' + str(port))
        if not os.path.exists(path):
            logging.info("Making Data directory: %s" % path)
            os.makedirs(path)

        # Create a keyFile for the --auth argument to mongod
        keyFilePath = os.path.join(path, 'keyFile')
        keyFile = open(keyFilePath, 'w+')
        keyFile.write('password1')
        keyFile.close()
        os.chmod(keyFilePath, int('0600', 8))

        member_logpath = os.path.join(self.logpath, 'db' + str(port) + '.log')
        if not os.path.exists(os.path.dirname(member_logpath)):
            logging.info("Making Log directory: %s" % os.path.dirname(member_logpath))
            os.makedirs(os.path.dirname(member_logpath))
        cmd = [
            mongod,
            '--dbpath', path,
            '--port', str(port),
            '--logpath', member_logpath,
            '--nojournal',
            # Various attempts to make startup faster on Mac by limiting
            # the size of files created at startup
            '--nohttpinterface',
            '--noprealloc',
            '--smallfiles',
            '--nssize', '1',
            '--oplogSize', '150', # 150MB oplog, not 5% of disk
            '-vvvvv',
            # Ensure that Mongo starts with all its features turned on so we
            # can test them!
            '--ipv6',
        ]
        # If auth is enabled (default), add auth mode
        if not noauth:
            cmd.extend(['--auth',
                        '--keyFile', keyFilePath,])

        assert not (master and slave), (
            "Conflicting arguments: can't be both master and slave"
        )

        assert bool(source_port) == bool(slave), (
            "Conflicting arguments: must provide 'slave' and 'source_port' or"
            " neither"
        )
        if master:
            cmd.append('--master')
        elif slave:
            cmd += ['--slave', '--source', 'localhost:' + str(source_port)]
        else:
            cmd += ['--replSet', self.name]

        logging.info('Starting %s' % ' '.join(cmd))
        proc = subprocess.Popen(cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT)
        res = self.wait_for(proc, port)
        if not res:
            raise Exception("Couldn't execute %s" % ' '.join(cmd))
        else:
            # Do TWO things!!
            # 1) Add member's info to global_state, in a format that rs_manager
            #    understands, so we can save it to disk at the end of this script
            #    and read it later
            # 2) Return member's info for use in replSetInitiate() in a format
            #    the Mongo server understands
            self.members_info[host] ={
                '_id': _id,
                'pid': proc.pid,
                'cmd': cmd,
                'logpath': member_logpath,
                'host': hostname,
                'port': port,
                'arbiter': arbiter,
                'master': master,
                'slave': slave,
                'source_port': source_port,
            }

            configitem = { '_id': _id, 'host': host, 'arbiterOnly': arbiter }
            configitem.update(extras)
            return configitem

    def start_set(self, fresh=False, restart=False, extra_options={}):
        if fresh:
            try:
                logging.info("Deleting dbpth '%s'" % self.dbpath)
                shutil.rmtree(self.dbpath)
            except OSError:
                # dbpath doesn't exist yet
                pass

            try:
                logging.info("Deleting logpath '%s'" % self.logpath)
                shutil.rmtree(self.logpath)
            except OSError:
                # logpath doesn't exist yet
                pass

        # If we just deleted the working dir (because it was equal to, or
        # beneath, the dbpath or logpath), then recreate the working directory
        # and change to the new copy of it.
        if not os.path.exists(self.home):
            logging.info(
                "I deleted my own working directory '%s', recreating it and "
                "chdir'ing to it" % self.home
            )
            os.makedirs(self.home)
            os.chdir(self.home)

        self.start_time = time.time()
        self.members = []
        for i in xrange(self.num_members):
            cur_port = self.port + i

            # If this is the last member we're starting, maybe make it arbiter
            arbiter = ( self.with_arbiter and (i==self.num_members-1))
            master = ( self.master_slave and i == 0)
            slave = ( self.master_slave and i > 0)

            source_port = self.port if slave else None

            # start_member() adds the member to global_state, and gives us some
            # JSON to include in replSetInitiate()
            member_info = self.start_member(
                port=cur_port, _id=i, arbiter=arbiter, master=master, slave=slave,
                source_port=source_port, noauth=self.noauth, extras=extra_options.get(i,{})
            )
            self.members.append(member_info)

        self.config = {'_id': self.name, 'members': self.members}
        self.primary = self.members[0]['host']

        if self.master_slave:
            # Is there any way to verify the pair is up?
            pass
        else:
            # Honestly, what's the hurry? members take a *long* time to start on EC2
            time.sleep(1)
            c = pymongo.Connection(self.primary)

            logging.info('Initiating replica set....')
            if not restart:
                c.admin.command('replSetInitiate', self.config)

            # Wait for all members to come online
            expected_secondaries = self.num_members - 1
            if self.with_arbiter: expected_secondaries -= 1
            expected_arbiters = 1 if self.with_arbiter else 0

            while True:
                time.sleep(2)

                try:
                    if (
                        len(self.get_primary()) == 1 and
                        len(self.get_secondaries()) == expected_secondaries and
                        len(self.get_arbiters()) == expected_arbiters
                    ):
                        break
                except pymongo.errors.AutoReconnect:
                    # Keep waiting
                    pass
        logging.info('Started %s members in %s seconds' % (
            self.num_members, int(time.time() - self.start_time)
        ))

        return self.primary

    def get_members_in_state(self, state):
        """
        @param state:       A state constant (1, 2, 3, ...) or 'any'
        @return:            RS members currently in that state, e.g.:
                                ['localhost:4000', 'localhost:4001']
        """
        if not self.members:
            logging.warning('No running members!')
            return []

        num_retries = 10
        while True:
            try:
                c = pymongo.Connection([m['host'] for m in self.members])
                break
            except (pymongo.errors.AutoReconnect, pymongo.errors.ConnectionFailure) as e:
                # Special case: all nodes down, or only arbiter (to which we are
                # banned from connecting) is up
                if state == 'any':
                    return global_state['members'].keys()
                else:
                    print "failed", e
                    num_retries -= 1
                    if num_retries == 0: 
                        raise
                    else:
                        time.sleep(5)
                        continue

        try:
            status = c.admin.command('replSetGetStatus')
            members = status['members']
            return [
                k['name'] for k in members
                if (k['state'] == state or state == 'any')
            ]
        except pymongo.errors.PyMongoError, e:
            logging.warning(e)
            return []
