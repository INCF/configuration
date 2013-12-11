#!/usr/bin/env python

import sys
from argparse import ArgumentParser
import subprocess
import time
from base64 import encodestring

try:
    import boto.ec2
    import boto.sqs
    from boto.vpc import VPCConnection
    from boto.exception import NoAuthHandlerFound
    from boto.sqs.message import RawMessage
except ImportError:
    print "boto required for script"
    sys.exit(1)


def run_cmd(cmd):
    if args.noop:
        sys.stderr.write('would have run: {}\n\n'.format(cmd))
    else:
        sys.stderr.write('running: {}\n'.format(cmd))
        process = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            shell=True)
        # don't buffer output
        for line in iter(process.stdout.readline, ""):
            sys.stderr.write(line)
            sys.stderr.flush()


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
    parser.add_argument('-v', '--vars', metavar="EXTRA_VAR_FILE",
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
    return parser.parse_args()


def main():

    security_group_id = None
    queue_name = "abbey-{}-{}".format(args.environment, args.deployment)

    try:
        sqs = boto.sqs.connect_to_region(args.region)
        ec2 = boto.ec2.connect_to_region(args.region)
    except NoAuthHandlerFound:
        print 'You must be able to connect to sqs and ec2 to use this script'
        sys.exit(1)

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

    # create the queue we will be listening on
    # in case it doesn't exist
    sqs_queue = sqs.create_queue(queue_name)

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
instance_id=$(curl http://169.254.169.254/latest/meta-data/instance-id 2>/dev/null)
instance_ip=$(curl http://169.254.169.254/latest/meta-data/local-ipv4 2>/dev/null)
instance_type=$(curl http://169.254.169.254/latest/meta-data/instance-type 2>/dev/null)

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
    /usr/bin/apt-get install -y git python-pip python-apt git-core build-essential python-dev libxml2-dev libxslt-dev curl --force-yes
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
secure_vars: "$base_dir/configuration-secure/ansible/vars/$environment/$environment-$deployment.yml"
edx_platform_commit: master
EOF

chmod 400 $secure_identity

$git_cmd clone -b $configuration_version https://github.com/edx/configuration

if $config_secure; then
    $git_cmd clone -b $configuration_secure_version git@github.com:edx/configuration-secure
fi

cd $base_dir/configuration
sudo pip install -r requirements.txt

cd $base_dir/configuration/playbooks/edx-east

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

    # res = ec2.run_instances(**ec2_args)
    sqs_queue.set_message_class(RawMessage)
#{u'ApproximateFirstReceiveTimestamp': u'1365474374620',
# u'ApproximateReceiveCount': u'2',
# u'SenderId': u'419278470775',
# u'SentTimestamp': u'1365474360357'}
    buf = []
    start_time = int(time.time())
    while True:
        messages = []
        while True:
            # get all available messages on the queue
            msgs = sqs_queue.get_messages(attributes='All')
            if not msgs:
                break
            messages.extend(msgs)

        for message in messages:
            msg_info = {
                'msg': message.get_body(),
                'sent_ts': float(message.attributes['SentTimestamp']) * .001,
                'recv_ts': float(message.attributes['ApproximateFirstReceiveTimestamp']) * .001,
            }
            buf.append(msg_info)
            sqs_queue.delete_message(message)

        now = int(time.time())
        new_buf = []
        output_buf = []
        for msg in sorted(buf, key=lambda k: k['sent_ts']):
            # only display messages that
            # have been in the buffer for
            # at least 5 seconds
            if now - msg['recv_ts'] >= 5:
                output_buf.append(msg)
            else:
                new_buf.append(msg)
        if output_buf:
            # print the output buffer
            print "\n".join([msg['msg'] for msg in sorted(output_buf, key=lambda k: k['sent_ts'])])

        # replace the buffer with
        # the left over messages
        buf = new_buf

        if not messages:
            # wait 1 second between sqs polls
            time.sleep(1)


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
    main()
