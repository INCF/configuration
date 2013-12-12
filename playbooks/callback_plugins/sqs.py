import os
import sys
import time
import json
try:
    import boto.sqs
    from boto.exception import NoAuthHandlerFound
except ImportError:
    print "Boto is required for the sqs_notify callback plugin"
    raise


class CallbackModule(object):
    def __init__(self):

        self.start_time = time.time()

        if 'ANSIBLE_ENABLE_SQS' in os.environ:
            self.enable_sqs = True
            if not 'SQS_REGION' in os.environ:
                print 'ANSIBLE_ENABLE_SQS enabled but SQS_REGION ' \
                      'not defined in environment'
                sys.exit(1)
            self.region = os.environ['SQS_REGION']
            try:
                self.sqs = boto.sqs.connect_to_region(self.region)
            except NoAuthHandlerFound:
                print 'ANSIBLE_ENABLE_SQS enabled but cannot connect ' \
                      'to AWS due invalid credentials'
                sys.exit(1)
            if not 'SQS_NAME' in os.environ:
                print 'ANSIBLE_ENABLE_SQS enabled but SQS_NAME not ' \
                      'defined in environment'
                sys.exit(1)
            self.name = os.environ['SQS_NAME']
            self.queue = self.sqs.create_queue(self.name)
            if 'SQS_MSG_PREFIX' in os.environ:
                self.prefix = os.environ['SQS_MSG_PREFIX'] + ': '
            else:
                self.prefix = ''
        else:
            self.enable_sqs = False

    def runner_on_failed(self, host, res, ignore_errors=False):
        if not ignore_errors:
            self._send_queue_message(res, 'FAILURE')

    def runner_on_ok(self, host, res):
        # don't send the setup results
        if res['invocation']['module_name'] != "setup":
            self._send_queue_message(res, 'OK')

    def playbook_on_task_start(self, name, is_conditional):
        self._send_queue_message(name, 'TASK')

    def playbook_on_play_start(self, pattern):
        self._send_queue_message(pattern, 'START')

    def playbook_on_stats(self, stats):
        d = {}
        delta = time.time() - self.start_time
        d['delta'] = delta
        for s in ['changed', 'failures', 'ok', 'processed', 'skipped']:
            d[s] = getattr(stats, s)
        self._send_queue_message(d, 'STATS')

    def _send_queue_message(self, msg, msg_type):
        delta = time.time() - self.start_time
        ts = '{:0>2.0f}:{:0>4.1f} '.format(delta / 60, delta % 60)
        msg['TS'] = ts
        msg['PREFIX'] = self.prefix
        if self.enable_sqs:
            self.sqs.send_message(json.dumps(msg))
