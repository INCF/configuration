#!/usr/bin/env python -u

import sys
from argparse import ArgumentParser
import time
import json
try:
    import boto.ec2
    import boto.sqs
    from boto.vpc import VPCConnection
    from boto.exception import NoAuthHandlerFound
    from boto.sqs.message import RawMessage
except ImportError:
    print "boto required for script"
    sys.exit(1)

class Unbuffered:
   def __init__(self, stream):
       self.stream = stream
   def write(self, data):
       self.stream.write(data)
       self.stream.flush()
   def __getattr__(self, attr):
       return getattr(self.stream, attr)

sys.stdout=Unbuffered(sys.stdout)

def parse_args():
    parser = ArgumentParser()
    parser.add_argument('--noop', action='store_true',
                        help="don't actually run the cmds",
                        default=False)
    parser.add_argument('--secure-vars', required=False,
                        metavar="SECURE_VAR_FILE",
                        help="path to secure-vars, defaults to "
                        "../../../configuration-secure/ansible/"
                        "vars/DEPLOYMENT/ENVIRONMENT.yml")
    parser.add_argument('--stack-name',
                        help="defaults to DEPLOYMENT-ENVIRONMENT",
                        metavar="STACK_NAME",
                        required=False)
    parser.add_argument('-p', '--play',
                        help='play name without the yml extension',
                        metavar="PLAY", required=True)
    parser.add_argument('-d', '--deployment', metavar="DEPLOYMENT",
                        required=True)
    parser.add_argument('-e', '--environment', metavar="ENVIRONMENT",
                        required=True)
    parser.add_argument('-v', '--verbose',
                        help="turn on verbosity", required=False)
    parser.add_argument('--vars', metavar="EXTRA_VAR_FILE",
                        help="path to extra var file", required=False)
    parser.add_argument('-a', '--application', required=False,
                        help="Application for subnet, defaults to admin",
                        default="admin")
    parser.add_argument('--configuration-version', required=False,
                        help="configuration repo version",
                        default="master")
    parser.add_argument('--configuration-secure-version', required=False,
                        help="configuration-secure repo version",
                        default="master")
    parser.add_argument('-j', '--jenkins-build', required=False,
                        help="jenkins build number to update")
    parser.add_argument('-b', '--base-ami', required=False,
                        help="ami to use as a base ami",
                        default="ami-0568456c")
    parser.add_argument('-i', '--identity', required=False,
                        help="path to identity file for pulling "
                             "down configuration-secure",
                        default=None)
    parser.add_argument('-r', '--region', required=False,
                        default="us-east-1",
                        help="aws region")
    parser.add_argument('-k', '--keypair', required=False,
                        default="deployment",
                        help="AWS keypair to use for instance")
    parser.add_argument('-t', '--instance-type', required=False,
                        default="m1.large",
                        help="instance type to launch")
    parser.add_argument("--security-group", required=False,
                        default="abbey", help="Security group to use")
    parser.add_argument("--role-name", required=False,
                        default="abbey",
                        help="IAM role name to use (must exist)")
    parser.add_argument("--msg-delay", required=False,
                        default=5,
                        help="How long to delay message display from sqs "
                             "to ensure ordering")
    return parser.parse_args()


def create_instance_args():

    security_group_id = None

    grp_details = ec2.get_all_security_groups()

    for grp in grp_details:
        if grp.name == args.security_group:
            security_group_id = grp.id
            break
    if not security_group_id:
        print "Unable to lookup id for security group {}".format(
            args.security_group)
        sys.exit(1)

    print "{:22} {:22}".format("stack_name", stack_name)
    print "{:22} {:22}".format("queue_name", queue_name)
    for value in ['region', 'base_ami', 'keypair',
                  'instance_type', 'security_group',
                  'role_name']:
        print "{:22} {:22}".format(value, getattr(args, value))

    vpc = VPCConnection()
    subnet = vpc.get_all_subnets(
        filters={
            'tag:aws:cloudformation:stack-name': stack_name,
            'tag:Application': args.application}
    )
    if len(subnet) != 1:
        sys.stderr.write("ERROR: Expected 1 admin subnet, got {}\n".format(
            len(subnet)))
        sys.exit(1)
    subnet_id = subnet[0].id

    print "{:22} {:22}".format("subnet_id", subnet_id)

    if args.identity:
        config_secure = 'true'
        with open(args.identity) as f:
            identity_file = f.read()
    else:
        config_secure = 'false'
        identity_file = "dummy"

    user_data = """#!/bin/bash
set -x
set -e
exec > >(tee /var/log/user-data.log|logger -t user-data -s 2>/dev/console) 2>&1
base_dir="/var/tmp/edx-cfg"
extra_vars="$base_dir/extra-vars-$$.yml"
secure_identity="$base_dir/secure-identity"
git_ssh="$base_dir/git_ssh.sh"
configuration_version="{configuration_version}"
configuration_secure_version="{configuration_secure_version}"
environment="{environment}"
deployment="{deployment}"
play="{play}"
config_secure={config_secure}
secure_vars_file="$base_dir/configuration-secure\\
/ansible/vars/$environment/$environment-$deployment.yml"
instance_id=\\
$(curl http://169.254.169.254/latest/meta-data/instance-id 2>/dev/null)
instance_ip=\\
$(curl http://169.254.169.254/latest/meta-data/local-ipv4 2>/dev/null)
instance_type=\\
$(curl http://169.254.169.254/latest/meta-data/instance-type 2>/dev/null)
playbook_dir="$base_dir/configuration/playbooks/edx-east"
git_repo="https://github.com/edx/configuration"
git_repo_secure="git@github.com:edx/configuration-secure"

if $config_secure; then
    git_cmd="env GIT_SSH=$git_ssh git"
else
    git_cmd="git"
fi

ANSIBLE_ENABLE_SQS=true
SQS_NAME={queue_name}
SQS_REGION=us-east-1
SQS_MSG_PREFIX="[ $instance_id $instance_ip $environment-$deployment $play ]"
PYTHONUNBUFFERED=1

# environment for ansible
export ANSIBLE_ENABLE_SQS SQS_NAME SQS_REGION SQS_MSG_PREFIX PYTHONUNBUFFERED

if [[ ! -x /usr/bin/git || ! -x /usr/bin/pip ]]; then
    echo "Installing pkg dependencies"
    /usr/bin/apt-get update
    /usr/bin/apt-get install -y git python-pip python-apt \\
        git-core build-essential python-dev libxml2-dev \\
        libxslt-dev curl --force-yes
fi


rm -rf $base_dir
mkdir -p $base_dir
cd $base_dir

cat << EOF > $git_ssh
#!/bin/sh
exec /usr/bin/ssh -o StrictHostKeyChecking=no -i "$secure_identity" "\$@"
EOF

chmod 755 $git_ssh

if $config_secure; then
    cat << EOF > $secure_identity
{identity_file}
EOF
fi

cat << EOF >> $extra_vars
---
secure_vars: $secure_vars_file
edx_platform_commit: master
xqueue_create_db: 'no'
EOF

chmod 400 $secure_identity

$git_cmd clone -b $configuration_version $git_repo

if $config_secure; then
    $git_cmd clone -b $configuration_secure_version \\
        $git_repo_secure
fi

cd $base_dir/configuration
sudo pip install -r requirements.txt

cd $playbook_dir

ansible-playbook -vvvv -c local -i "localhost," $play.yml -e@$extra_vars

rm -rf $base_dir

    """.format(
                configuration_version=args.configuration_version,
                configuration_secure_version=args.configuration_secure_version,
                environment=args.environment,
                deployment=args.deployment,
                play=args.play,
                config_secure=config_secure,
                identity_file=identity_file,
                queue_name=queue_name)

    ec2_args = {
        'security_group_ids': [security_group_id],
        'subnet_id': subnet_id,
        'key_name': args.keypair,
        'image_id': args.base_ami,
        'instance_type': args.instance_type,
        'instance_profile_name': args.role_name,
        'user_data': user_data,
    }

    return ec2_args


def poll_sqs():
    oldest_msg_ts = 0
    buf = []
    while True:
        messages = []
        while True:
            # get all available messages on the queue
            msgs = sqs_queue.get_messages(attributes='All')
            if not msgs:
                break
            messages.extend(msgs)

        for message in messages:
            recv_ts = float(
                message.attributes['ApproximateFirstReceiveTimestamp']) * .001
            sent_ts = float(message.attributes['SentTimestamp']) * .001
            try:
                msg_info = {
                    'msg': json.loads(message.get_body()),
                    'sent_ts': sent_ts,
                    'recv_ts': recv_ts,
                }
                buf.append(msg_info)
            except ValueError as e:
                print "!!! ERROR !!! unable to parse queue message, " \
                      "expecting valid json: {} : {}".format(
                          message.get_body(), e)
            if not oldest_msg_ts or recv_ts < oldest_msg_ts:
                oldest_msg_ts = recv_ts
            sqs_queue.delete_message(message)

        now = int(time.time())
        if buf:
            if (now - max([msg['recv_ts'] for msg in buf])) > args.msg_delay:
                # sort by TS instead of recv_ts
                # because the sqs timestamp is not as
                # accurate
                buf.sort(key=lambda k: k['msg']['TS'])
                to_disp = buf.pop(0)
                if 'TASK' in to_disp['msg']:
                    print "\n{} {} : {}".format(
                        to_disp['msg']['TS'],
                        to_disp['msg']['PREFIX'],
                        to_disp['msg']['TASK']),
                elif 'OK' in to_disp['msg']:
                    if to_disp['msg']['OK']['changed']:
                        changed = "*OK*"
                    else:
                        changed = "OK"
                    print " {} ({})".format(
                        changed, to_disp['msg']['delta']),
                    if args.verbose:
                        for key, value in to_disp['msg']['OK'].iteritems():
                            print "    {<15}{}".format(key, value)
                elif 'FAILURE' in to_disp['msg']:
                    print " !!!! FAILURE !!!!",
                    for key, value in to_disp['msg']['FAILURE'].iteritems():
                        print "    {<15}{}".format(key, value)
                elif 'STATS' in to_disp['msg']:
                    print "\n{} {} : COMPLETE".format(
                        to_disp['msg']['TS'],
                        to_disp['msg']['PREFIX'])

        if not messages:
            # wait 1 second between sqs polls
            time.sleep(1)

#def create_ami(instance_id, name, description):
#    params = {'instance_id': instance_id,
#              'name': name,
#              'description': description,
#              'no_reboot': True}
#
#    image_id = ec2.create_image(**params)
#
#    for i in range(30):
#        try:
#            img = ec2.get_image(image_id)
#            break
#        except boto.exception.EC2ResponseError as e:
#            if e.error_code == 'InvalidAMIID.NotFound':
#                time.sleep(1)
#            else:
#                raise
#    else:
#        module.fail_json(msg = "timed out waiting for image to be recognized")
#
#    # wait here until the image is created
#    wait_timeout = time.time() + wait_timeout
#    while wait and wait_timeout > time.time() and (img is None or img.state != 'available'):
#        img = ec2.get_image(image_id)
#        time.sleep(3)
#    if wait and wait_timeout <= time.time():
#        # waiting took too long
#        module.fail_json(msg = "timed out waiting for image to be created")
#
#    module.exit_json(msg="AMI creation operation complete", image_id=image_id, state=img.state, changed=True)
#    sys.exit(0)
#


if __name__ == '__main__':

    args = parse_args()
    if args.secure_vars:
        secure_vars = args.secure_vars
    else:
        secure_vars = "../../../configuration-secure/" \
                      "ansible/vars/{}/{}.yml".format(
                      args.deployment, args.environment)
    if args.stack_name:
        stack_name = args.stack_name
    else:
        stack_name = "{}-{}".format(args.environment, args.deployment)

    try:
        sqs = boto.sqs.connect_to_region(args.region)
        ec2 = boto.ec2.connect_to_region(args.region)
    except NoAuthHandlerFound:
        print 'You must be able to connect to sqs and ec2 to use this script'
        sys.exit(1)

    try:
        sqs_queue = None
        queue_name = "abbey-{}-{}-{}".format(
            args.environment, args.deployment, int(time.time() * 100))

        # create the queue we will be listening on
        # in case it doesn't exist
        sqs_queue = sqs.create_queue(queue_name)
        sqs_queue.set_message_class(RawMessage)

        instance_id = None
        ec2_args = create_instance_args()
        res = ec2.run_instances(**ec2_args)
        instance_id = res.instances[0].id

        poll_sqs()

    except (KeyboardInterrupt, Exception) as e:
        if sqs_queue:
            print "Removing queue - {}".format(queue_name)
            sqs.delete_queue(sqs_queue)
        if instance_id:
            print "Terminating instance ID - {}".format(instance_id)
            ec2.terminate_instances(instance_ids=[instance_id])
        raise e
