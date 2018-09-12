import os
import pkg_resources
from . import verdictresult
from py4j.java_gateway import JavaGateway
from time import sleep


class VerdictContext:
    """The main Python interface to VerdictDB Java core.

    The path to the jdbc drivers must be specified to use them.

    Args:
        extra_class_path: The extra classpath used in addition to verdictdb's jar file. This arg
                          can either be a single str or a list of str; each str is an absolute path
                          to a jar file.
    """

    def __init__(self, url, extra_class_path):
        self._gateway = self._get_gateway(extra_class_path)
        self._context = self._get_context(self._gateway, url)

    def sql(self, query):
        return verdictresult.SingleResultSet(self._context.sql(query))

    def _get_gateway(self, extra_class_path):
        """
        Initializes a py4j gateway.

        Args:
            class_path: Either a single str or a list of str; each str is an absolute path to the
            jar file.
        """
        class_path = self._get_class_path(extra_class_path)
        gateway = JavaGateway.launch_gateway(classpath=class_path, die_on_exit=True)
        sleep(1)
        return gateway

    def _get_class_path(self, extra_class_path):
        """
        Returns str class path including the one for verdict jar path
        """
        verdict_jar_path = self._get_verdict_jar_path()
        str_class_path = None
        if isinstance(extra_class_path, str):
            str_class_path = '{}:{}'.format(extra_class_path, verdict_jar_path)
        elif isinstance(extra_class_path, list):
            extra_class_path_str = ':'.join(extra_class_path)
            str_class_path = '{}:{}'.format(extra_class_path_str, verdict_jar_path)
        return str_class_path

    def _get_verdict_jar_path(self):
        root_dir = os.path.dirname(os.path.abspath(__file__))
        lib_dir = os.path.join(root_dir, 'lib')
        version = self._get_version();
        verdict_jar_file = 'verdictdb-core-{}-jar-with-dependencies.jar'.format(version)
        verdict_jar_path = os.path.join(lib_dir, verdict_jar_file)
        return verdict_jar_path

    def _get_version(self):
        return pkg_resources.require("pyverdict")[0].version

    def _get_context(self, gateway, url):
        return gateway.jvm.org.verdictdb.VerdictContext.fromConnectionString(url)
