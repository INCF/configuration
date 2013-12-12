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
                self.prefix = os.environ['SQS_MSG_PREFIX']
            else:
                self.prefix = ''

            self.last_seen_ts = {}
        else:
            self.enable_sqs = False


    def runner_on_failed(self, host, res, ignore_errors=False):
        if not self.enable_sqs:
            return
        if not ignore_errors:
            self._send_queue_message(res, 'FAILURE')

    def runner_on_ok(self, host, res):
        if not self.enable_sqs:
            return
        # don't send the setup results
        if res['invocation']['module_name'] != "setup":
            self._send_queue_message(res, 'OK')

    def playbook_on_task_start(self, name, is_conditional):
        if not self.enable_sqs:
            return
        self._send_queue_message(name, 'TASK')

    def playbook_on_play_start(self, pattern):
        if not self.enable_sqs:
            return
        self._send_queue_message(pattern, 'START')

    def playbook_on_stats(self, stats):
        if not self.enable_sqs:
            return
        d = {}
        delta = time.time() - self.start_time
        d['delta'] = delta
        for s in ['changed', 'failures', 'ok', 'processed', 'skipped']:
            d[s] = getattr(stats, s)
        self._send_queue_message(d, 'STATS')

    def _send_queue_message(self, msg, msg_type):
        if not self.enable_sqs:
            return
        from_start = time.time() - self.start_time
        payload = {msg_type: msg}
        payload['TS'] = from_start
        payload['PREFIX'] = self.prefix
        # update the last seen timestamp for
        # the message type
        self.last_seen_ts[msg_type] = time.time()
        if msg_type in ['OK', 'FAILURE']:
            # report the delta between the OK/FAILURE and
            # last TASK
            if 'TASK' in self.last_seen_ts:
                from_task = \
                    self.last_seen_ts[msg_type] - self.last_seen_ts['TASK']
                payload['delta'] = from_task
        self.sqs.send_message(self.queue, json.dumps(payload))
