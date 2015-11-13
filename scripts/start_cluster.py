
from boto import ec2;
import csv;
import sys;
import pipes;
import os;
import subprocess;
import time;
import itertools
from sys import stderr;
from datetime import datetime

key_pair='Pils pair'
id_file="/home/jfc/.ssh/dss2_rsa"

region="us-west-2"
zone="us-west-2a"

class Options:
    user = None
    identity_file = None
    private_ips = False

core_site = """
<configuration>
    <property>
        <name>fs.default.name</name>
        <value>hdfs://%s:9000</value>
    </property>
</configuration>
"""

def start_instances(cluster, id_file, genkeys=False, region=region, zone=zone, core_site=core_site):
    opts = Options();
    opts.user = "ec2-user"
    opts.identity_file = id_file
    aaki = os.getenv('AWS_ACCESS_KEY_ID')
    asak = os.getenv('AWS_SECRET_ACCESS_KEY')
    if aaki is None:
        print("ERROR: The environment variable AWS_ACCESS_KEY_ID must be set")
        sys.exit(1)
    if asak is None:
        print("ERROR: The environment variable AWS_SECRET_ACCESS_KEY must be set")
        sys.exit(1)
    conn = ec2.connect_to_region(region, aws_access_key_id=aaki, aws_secret_access_key=asak);

    (masters, slaves) = get_existing_cluster(conn, region, cluster);

    allnodes = masters + slaves
    for node in allnodes:
        if node.state not in ['running', 'stopping']:
            node.start();

    wait_for_cluster_state(
        conn=conn,
        opts=opts,
        cluster_instances=allnodes,
        cluster_state='ssh-ready'
    )

    master = get_dns_name(masters[0], opts.private_ips);

    print("configuring master %s" % master);

    slave_names = [get_dns_name(i, opts.private_ips) for i in slaves];
    slaves_string = reduce(lambda x,y : x + "\n" + y, slave_names);
    
    hscommand="echo -e '%s' > /opt/spark/conf/slaves" % slaves_string
    ssh(master, opts, hscommand.encode('ascii','ignore'))

    sscommand="echo -e '%s' > /usr/local/hadoop/etc/hadoop/slaves" % slaves_string
    ssh(master, opts, sscommand.encode('ascii','ignore'))

    core_conf = core_site % master
    hccommand="echo '%s' >  /usr/local/hadoop/etc/hadoop/core-site.xml" % core_conf
    ssh(master, opts, hccommand.encode('ascii','ignore'))

    ssh(master, opts, """rm -f ~/.ssh/known_hosts""")
    for slave in slave_names:
        print("configuring slave %s" % slave)
        ssh(slave, opts, """rm -f ~/.ssh/known_hosts""")
        ssh(slave, opts, hccommand.encode('ascii','ignore'))
        ssh(slave, opts, sscommand.encode('ascii','ignore'))

    if (genkeys):
        print("Generating cluster's SSH key on master...")
        ssh(master, opts, """rm -f ~/.ssh/id_rsa""")
        key_setup = """
            (ssh-keygen -q -t rsa -N '' -f ~/.ssh/id_rsa &&
             cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys)
        """
        ssh(master, opts, key_setup)
        dot_ssh_tar = ssh_read(master, opts, ['tar', 'c', '.ssh'])
        print("Transferring cluster's SSH key to slaves...")
        for slave in slave_names:
            ssh_write(slave, opts, ['tar', 'x'], dot_ssh_tar)


    

def get_dns_name(instance, private_ips=False):
    dns = instance.public_dns_name if not private_ips else \
        instance.private_ip_address
    return dns


# Backported from Python 2.7 for compatiblity with 2.6 (See SPARK-1990)
def _check_output(*popenargs, **kwargs):
    if 'stdout' in kwargs:
        raise ValueError('stdout argument not allowed, it will be overridden.')
    process = subprocess.Popen(stdout=subprocess.PIPE, *popenargs, **kwargs)
    output, unused_err = process.communicate()
    retcode = process.poll()
    if retcode:
        cmd = kwargs.get("args")
        if cmd is None:
            cmd = popenargs[0]
        raise subprocess.CalledProcessError(retcode, cmd, output=output)
    return output


def ssh_read(host, opts, command):
    return _check_output(
        ssh_command(opts) + ['%s@%s' % (opts.user, host), stringify_command(command)])



def ssh_write(host, opts, command, arguments):
    tries = 0
    while True:
        proc = subprocess.Popen(
            ssh_command(opts) + ['%s@%s' % (opts.user, host), stringify_command(command)],
            stdin=subprocess.PIPE)
        proc.stdin.write(arguments)
        proc.stdin.close()
        status = proc.wait()
        if status == 0:
            break
        elif tries > 5:
            raise RuntimeError("ssh_write failed with error %s" % proc.returncode)
        else:
            print("Error {0} while executing remote command, retrying after 30 seconds".
                  format(status))
            time.sleep(30)
            tries = tries + 1


def copy_files(fromfile, opts, node, tofile):
    command = [
        'rsync', '-rv',
        '-e', stringify_command(ssh_command(opts)),
        "%s" % fromfile,
        "%s@%s:%s" % (opts.user, node, tofile)
    ]
    subprocess.check_call(command)



def get_existing_cluster(conn, region, cluster_name, die_on_error=True):
    """
    Get the EC2 instances in an existing cluster if available.
    Returns a tuple of lists of EC2 instance objects for the masters and slaves.
    """
    print("Searching for existing cluster {c} in region {r}...".format(
          c=cluster_name, r=region))

    def get_instances(group_names):
        """
        Get all non-terminated instances that belong to any of the provided security groups.

        EC2 reservation filters and instance states are documented here:
            http://docs.aws.amazon.com/cli/latest/reference/ec2/describe-instances.html#options
        """
        reservations = conn.get_all_reservations(
            filters={"instance.group-name": group_names})
        instances = itertools.chain.from_iterable(r.instances for r in reservations)
        return [i for i in instances if i.state not in ["shutting-down", "terminated"]]

    master_instances = get_instances([cluster_name + "-master"])
    slave_instances = get_instances([cluster_name + "-slaves"])

    if any((master_instances, slave_instances)):
        print("Found {m} master{plural_m}, {s} slave{plural_s}.".format(
              m=len(master_instances),
              plural_m=('' if len(master_instances) == 1 else 's'),
              s=len(slave_instances),
              plural_s=('' if len(slave_instances) == 1 else 's')))

    if not master_instances and die_on_error:
        print("ERROR: Could not find a master for cluster {c} in region {r}.".format(
              c=cluster_name, r=region))
        sys.exit(1)

    return (master_instances, slave_instances)


def stringify_command(parts):
    if isinstance(parts, str):
        return parts
    else:
        return ' '.join(map(pipes.quote, parts))

def ssh_args(opts):
    parts = ['-o', 'StrictHostKeyChecking=no']
    if opts.identity_file is not None:
        parts += ['-i', opts.identity_file]
    return parts

def ssh_command(opts):
    return ['ssh'] + ssh_args(opts)

def ssh(host, opts, command):
    tries = 0
    while True:
        try:
            return subprocess.check_call(
                ssh_command(opts) + ['-t', '-t', '%s@%s' % (opts.user, host),
                                     stringify_command(command)])
        except subprocess.CalledProcessError as e:
            if tries > 5:
                # If this was an ssh failure, provide the user with hints.
                if e.returncode == 255:
                    raise UsageError(
                        "Failed to SSH to remote host {0}.\n" +
                        "Please check that you have provided the correct --identity-file and " +
                        "--key-pair parameters and try again.".format(host))
                else:
                    raise e
            print >> stderr, \
                "Error executing remote command, retrying after 30 seconds: {0}".format(e)
            time.sleep(30)
            tries = tries + 1

def wait_for_cluster_state(conn, opts, cluster_instances, cluster_state):
    """
    Wait for all the instances in the cluster to reach a designated state.

    cluster_instances: a list of boto.ec2.instance.Instance
    cluster_state: a string representing the desired state of all the instances in the cluster
           value can be 'ssh-ready' or a valid value from boto.ec2.instance.InstanceState such as
           'running', 'terminated', etc.
           (would be nice to replace this with a proper enum: http://stackoverflow.com/a/1695250)
    """
    sys.stdout.write(
        "Waiting for cluster to enter '{s}' state.".format(s=cluster_state)
    )
    sys.stdout.flush()

    start_time = datetime.now()

    num_attempts = 0

    while True:
        time.sleep(5 * num_attempts)  # seconds

        for i in cluster_instances:
            i.update()

        statuses = conn.get_all_instance_status(instance_ids=[i.id for i in cluster_instances])

        if cluster_state == 'ssh-ready':
            if all(i.state == 'running' for i in cluster_instances) and \
               all(s.system_status.status == 'ok' for s in statuses) and \
               all(s.instance_status.status == 'ok' for s in statuses) and \
               is_cluster_ssh_available(cluster_instances, opts):
                break
        else:
            if all(i.state == cluster_state for i in cluster_instances):
                break

        num_attempts += 1

        sys.stdout.write(".")
        sys.stdout.flush()

    sys.stdout.write("\n")

    end_time = datetime.now()
    print "Cluster is now in '{s}' state. Waited {t} seconds.".format(
        s=cluster_state,
        t=(end_time - start_time).seconds
    )

def is_ssh_available(host, opts):
    """
    Check if SSH is available on a host.
    """
    try:
        with open(os.devnull, 'w') as devnull:
            ret = subprocess.check_call(
                ssh_command(opts) + ['-t', '-t', '-o', 'ConnectTimeout=3',
                                     '%s@%s' % (opts.user, host), stringify_command('true')],
                stdout=devnull,
                stderr=devnull
            )
        return ret == 0
    except subprocess.CalledProcessError as e:
        return False


def is_cluster_ssh_available(cluster_instances, opts):
    """
    Check if SSH is available on all the instances in a cluster.
    """
    for i in cluster_instances:
        if not is_ssh_available(host=i.ip_address, opts=opts):
            return False
    else:
        return True


start_instances("bidcluster3", id_file, False)

#print ec2.regions()
# conn = ec2.connect_to_region("us-west-2", aws_access_key_id=aaki, aws_secret_access_key=asak);


