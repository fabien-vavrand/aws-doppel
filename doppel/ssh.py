import io
import paramiko

from retry import retry
from fabric import Connection
from contextlib import contextmanager


class SshSession:

    def __init__(self, host, user, key=None):
        self.host = host
        self.port = 22
        self.user = user
        self.key = key
        self.pkey = paramiko.RSAKey.from_private_key(io.StringIO(self.key))
        self.connection = Connection(self.host, user=self.user, port=22, connect_kwargs={'pkey': self.pkey})

    @retry(tries=5, delay=10)
    def connect(self):
        self.connection.open()

    def run(self, command, **kwargs):
        # http://docs.pyinvoke.org/en/latest/api/runners.html#invoke.runners.Runner.run
        if not self.connection.is_connected:
            self.connect()

        self.connection.run(command, hide=True, **kwargs)

    def mkdir(self, path, **kwargs):
        self.connection.run('mkdir {}'.format(path), **kwargs)

    def python(self, code, **kwargs):
        self.connection.run('python {}'.format(code), **kwargs)

    @contextmanager
    def cd(self, *path):
        cd_path = '/'.join(list(path))
        self.connection.command_cwds.append(cd_path)
        try:
            yield
        finally:
            self.connection.command_cwds.pop()

    @contextmanager
    def activate(self, env):
        self.connection.command_prefixes.append('conda activate {}'.format(env))
        try:
            yield
        finally:
            self.connection.command_prefixes.pop()
