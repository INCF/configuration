import os
import sys
import time
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

    def on_any(self, *args, **kwargs):
        pass

    def runner_on_failed(self, host, res, ignore_errors=False):
        if not ignore_errors:
            message = "FAILURE: {}".format(host)
            self._send_queue_message(message)

    def runner_on_ok(self, host, res):
        pass

    def runner_on_error(self, host, msg):
        pass

    def runner_on_skipped(self, host, item=None):
        pass

    def runner_on_unreachable(self, host, res):
        pass

    def runner_on_no_hosts(self):
        pass

    def runner_on_async_poll(self, host, res, jid, clock):
        pass

    def runner_on_async_ok(self, host, res, jid):
        pass

    def runner_on_async_failed(self, host, res, jid):
        pass

    def playbook_on_start(self):
        pass

    def playbook_on_notify(self, host, handler):
        pass

    def playbook_on_no_hosts_matched(self):
        pass

    def playbook_on_no_hosts_remaining(self):
        pass

    def playbook_on_task_start(self, name, is_conditional):
        message = "TASK: {}".format(name)
        self._send_queue_message(message)

    def playbook_on_setup(self):
        pass

    def playbook_on_import_for_host(self, host, imported_file):
        pass

    def playbook_on_not_import_for_host(self, host, missing_file):
        pass

    def playbook_on_play_start(self, pattern):
        pass
        message = "Starting play {}".format(pattern)
        self._send_queue_message(message)

    def playbook_on_stats(self, stats):
        pass

    def _send_queue_message(self, message):
        delta = time.time() - self.start_time
        ts = '{:0>2.0f}:{:0>4.1f} '.format(delta / 60, delta % 60)
        if self.enable_sqs:
            self.sqs.send_message(self.queue, ts + self.prefix +
                                  message.encode('utf-8'))
