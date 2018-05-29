#
# Cassandra Cluster Management lib
#

from __future__ import absolute_import

import copy
import logging
import os
import platform
import re
import shutil
import signal
import socket
import stat
import subprocess
import sys
import time
from distutils.version import LooseVersion  #pylint: disable=import-error, no-name-in-module

import six
import yaml
from six import print_, string_types

BIN_DIR = "bin"
CASSANDRA_CONF_DIR = "conf"
DSE_CASSANDRA_CONF_DIR = "resources/cassandra/conf"
OPSCENTER_CONF_DIR = "conf"

CASSANDRA_CONF = "cassandra.yaml"
JVM_OPTS_PATTERN = "jvm*.options"
LOG4J_CONF = "log4j-server.properties"
LOG4J_TOOL_CONF = "log4j-tools.properties"
LOGBACK_CONF = "logback.xml"
LOGBACK_TOOLS_CONF = "logback-tools.xml"
CASSANDRA_ENV = "cassandra-env.sh"
JVM_DEPENDENT_SH = "jvm-dependent.sh"  # since DSE 6.8
CASSANDRA_WIN_ENV = "cassandra-env.ps1"
CASSANDRA_SH = "cassandra.in.sh"

CONFIG_FILE = "config"
CCM_CONFIG_DIR = "CCM_CONFIG_DIR"

def get_options_removal_dict(options):
    dict = {}
    for option in options:
        dict[option] = None
    return dict

#Options introduced in 4.0
CCM_40_YAML_OPTIONS = get_options_removal_dict(['repaired_data_tracking_for_range_reads_enabled',
                  'corrupted_tombstone_strategy',
                  'repaired_data_tracking_for_partition_reads_enabled',
                  'report_unconfirmed_repaired_data_mismatches'])

class InfoFilter(logging.Filter):
    def filter(self, rec):
        return rec.levelno in (logging.DEBUG, logging.INFO)

LOG_FMT = '%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s'
DATE_FMT = '%H:%M:%S'
FORMATTER = logging.Formatter(LOG_FMT, DATE_FMT)

INFO_HANDLER = logging.StreamHandler(sys.stdout)
INFO_HANDLER.setLevel(logging.DEBUG)
INFO_HANDLER.addFilter(InfoFilter())
INFO_HANDLER.setFormatter(FORMATTER)

ERROR_HANDLER = logging.StreamHandler(sys.stderr)
ERROR_HANDLER.setLevel(logging.WARNING)
ERROR_HANDLER.setFormatter(FORMATTER)

LOG = logging.getLogger('ccm')
LOG.setLevel(logging.DEBUG)
LOG.addHandler(INFO_HANDLER)
LOG.addHandler(ERROR_HANDLER)

# If set to True, a node should not rotate logfiles to make sure, that intermittent test
# failures due to log file rotation don't happen. See DB-2759.
DONT_ROTATE_LOGFILES = False


def error(msg):
    LOG.error(msg)


def warning(msg):
    LOG.warning(msg)


def info(msg):
    LOG.info(msg)


def debug(msg):
    LOG.debug(msg)


class CCMError(Exception):
    pass


class LoadError(CCMError):
    pass


class ArgumentError(CCMError):
    pass


class UnavailableSocketError(CCMError):
    pass


class TimeoutError(Exception):

    def __init__(self, data):
        Exception.__init__(self, str(data))


class LogPatternToVersion(object):

    def __init__(self, versions_to_patterns, default_pattern=None):
        self.versions_to_patterns, self.default_pattern = versions_to_patterns, default_pattern

    def __call__(self, version):
        keys_less_than_version = [k for k in self.versions_to_patterns if k <= version]

        if not keys_less_than_version:
            if self.default_pattern is not None:
                return self.default_pattern
            else:
                raise ValueError("Some kind of default pattern must be specified!")

        return self.versions_to_patterns[max(keys_less_than_version, key=lambda v: LooseVersion(v) if not isinstance(v, LooseVersion) else v)]

    def __repr__(self):
        return str(self.__class__) + "(versions_to_patterns={}, default_pattern={})".format(self.versions_to_patterns, self.default_pattern)

    @property
    def patterns(self):
        patterns = list(self.versions_to_patterns.values())
        if self.default_pattern is not None:
            patterns = patterns + [self.default_pattern]
        return patterns

    @property
    def versions(self):
        return list(self.versions_to_patterns)


def get_default_path():
    if CCM_CONFIG_DIR in os.environ and os.environ[CCM_CONFIG_DIR]:
        default_path = os.environ[CCM_CONFIG_DIR]
    else:
        default_path = os.path.join(get_user_home(), '.ccm')

    if not os.path.exists(default_path):
        os.mkdir(default_path)
    return default_path


def get_default_path_display_name():
    default_path = get_default_path().lower()
    user_home = get_user_home().lower()

    if default_path.startswith(user_home):
        default_path = os.path.join('~', default_path[len(user_home) + 1:])

    return default_path


def get_user_home():
    if is_win():
        if sys.platform == "cygwin":
            # Need the fully qualified directory
            output = subprocess.Popen(["cygpath", "-m", os.path.expanduser('~')], stdout=subprocess.PIPE, stderr=subprocess.STDOUT).communicate()[0].rstrip()
            return output
        else:
            return os.environ['USERPROFILE']
    else:
        return os.path.expanduser('~')


def get_config():
    config_path = os.path.join(get_default_path(), CONFIG_FILE)
    if not os.path.exists(config_path):
        return {}

    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


def now_ms():
    return int(round(time.time() * 1000))


def parse_interface(itf, default_port):
    i = itf.split(':')
    if len(i) == 1:
        return (i[0].strip(), default_port)
    elif len(i) == 2:
        return (i[0].strip(), int(i[1].strip()))
    else:
        raise ValueError("Invalid interface definition: " + itf)


def current_cluster_name(path):
    try:
        with open(os.path.join(path, 'CURRENT'), 'r') as f:
            return f.readline().strip()
    except IOError:
        return None


def switch_cluster(path, new_name):
    with open(os.path.join(path, 'CURRENT'), 'w') as f:
        f.write(new_name + '\n')


def substitute_in_files(files, substitutions):
    """
    Replaces one or more patterns in one or more files. Unlike replace_in_file this method does
    not replace the whole line but just the parts of the line that matches the regular expression.
    Operates on a per-line basis, like replace_in_file, so greedy matches do match at max a single
    line in a file.
    Unlike replace_in_file, non-existing files are ignored.

    Example invocations:
        common.substitute_in_files('some_file_name', [regex, substritute])
        common.substitute_in_files('some_file_name', [[regex_a, substritute_for_a],
                                                      [regex_b, substritute_for_b]])
        common.substitute_in_files([filename_a, filename_b], [regex, substritute])
        common.substitute_in_files([filename_a, filename_b], [[regex_a, substritute_for_a],
                                                              [regex_b, substritute_for_b]])

    :param files: can be a single filename or a list of filenames
    :param substitutions: can be a single substitution, pair of pattern and replacement, or a list of substritutions
    """

    # ensure files is a list of files
    files = files if isinstance(files, list) else [files]

    if not isinstance(substitutions, list) or len(substitutions) == 0:
        raise ValueError("invalid value in substitutions")
    # make substitutions a list of lists in case the caller just provided a single [regex, substitute] instead
    # of a list of those
    if len(substitutions) == 2 and not isinstance(substitutions[0], list) and not isinstance(substitutions[1], list):
        substitutions = [substitutions]

    # validate substitutions parameter
    for subst in substitutions:
        if not isinstance(subst, list) or len(subst) != 2:
            raise ValueError("invalid value in substitutions")
        subst[0] = re.compile(subst[0])

    for file in files:
        if os.path.exists(file):
            file_tmp = file + "." + str(os.getpid()) + ".tmp"
            try:
                with open(file, 'r') as f:
                    with open(file_tmp, 'w') as f_tmp:
                        _substitute_in_list(f, substitutions, lambda line: f_tmp.write(line))
                shutil.move(file_tmp, file)
            finally:
                # delete the temp file, if either the substitution of shutil.move fails
                if os.path.exists(file_tmp):
                    os.unlink(file_tmp)


def _substitute_in_list(input, substitutions, output):
    for line in input:
        for pat, sub in substitutions:
            line = pat.sub(sub, line)
        output(line)


def replace_in_file(file, regexp, replace):
    replaces_in_file(file, [(regexp, replace)])


def replaces_in_file(file, replacement_list):
    rs = [(re.compile(regexp), repl) for (regexp, repl) in replacement_list]
    file_tmp = file + "." + str(os.getpid()) + ".tmp"
    with open(file, 'r') as f:
        with open(file_tmp, 'w') as f_tmp:
            for line in f:
                for r, replace in rs:
                    match = r.search(line)
                    if match:
                        line = replace + "\n"
                f_tmp.write(line)
    shutil.move(file_tmp, file)


def replace_or_add_into_file_tail(file, regexp, replace):
    replaces_or_add_into_file_tail(file, [(regexp, replace)])


def replaces_or_add_into_file_tail(file, replacement_list, add_config_close=True):
    rs = [(re.compile(regexp), repl) for (regexp, repl) in replacement_list]
    is_line_found = False
    file_tmp = file + "." + str(os.getpid()) + ".tmp"
    with open(file, 'r') as f:
        with open(file_tmp, 'w') as f_tmp:
            for line in f:
                for r, replace in rs:
                    match = r.search(line)
                    if match:
                        line = replace + "\n"
                        is_line_found = True
                if "</configuration>" not in line:
                    f_tmp.write(line)
            # In case, entry is not found, and need to be added
            if not is_line_found:
                f_tmp.write('\n' + replace + "\n")
            # We are moving the closing tag to the end of the file.
            # Previously, we were having an issue where new lines we wrote
            # were appearing after the closing tag, and thus being ignored.
            if add_config_close:
                f_tmp.write("</configuration>\n")

    shutil.move(file_tmp, file)


def rmdirs(path):
    if is_win():
        # Handle Windows 255 char limit
        shutil.rmtree(u"\\\\?\\" + path)
    else:
        shutil.rmtree(path)


def make_cassandra_env(install_dir, node_path, update_conf=True):
    if is_win() and get_version_from_build(node_path=node_path) >= '2.1':
        sh_file = os.path.join(CASSANDRA_CONF_DIR, CASSANDRA_WIN_ENV)
    else:
        sh_file = os.path.join(BIN_DIR, CASSANDRA_SH)
    orig = os.path.join(install_dir, sh_file)
    dst = os.path.join(node_path, sh_file)
    if not is_win() or not os.path.exists(dst):
        shutil.copy(orig, dst)

    if update_conf and not (is_win() and get_version_from_build(node_path=node_path) >= '2.1'):
        replacements = [
            ('CASSANDRA_HOME=', '\tCASSANDRA_HOME=%s' % install_dir),
            ('CASSANDRA_CONF=', '\tCASSANDRA_CONF=%s' % os.path.join(node_path, 'conf'))
        ]
        replaces_in_file(dst, replacements)

    # If a cluster-wide cassandra.in.sh file exists in the parent
    # directory, append it to the node specific one:
    cluster_sh_file = os.path.join(node_path, os.path.pardir, 'cassandra.in.sh')
    if os.path.exists(cluster_sh_file):
        append = open(cluster_sh_file).read()
        with open(dst, 'a') as f:
            f.write('\n\n### Start Cluster wide config ###\n')
            f.write(append)
            f.write('\n### End Cluster wide config ###\n\n')

    env = os.environ.copy()
    env['CASSANDRA_INCLUDE'] = os.path.join(dst)
    env['MAX_HEAP_SIZE'] = os.environ.get('CCM_MAX_HEAP_SIZE', '500M')
    env['HEAP_NEWSIZE'] = os.environ.get('CCM_HEAP_NEWSIZE', '50M')
    env['MAX_DIRECT_MEMORY'] = os.environ.get('CCM_MAX_DIRECT_SIZE', '2048M') # no effect before 6.0
    env['CASSANDRA_HOME'] = install_dir
    env['CASSANDRA_CONF'] = os.path.join(node_path, 'conf')

    if CASSANDRA_CONF != 'cassandra.yaml':
        # the yaml file can be changed because of different deployments, in which case we must ensure
        # that standalone tools also use it, since otherwise they would use the default cassandra.yaml
        # whose folders point to the wrong location since only CASSANDRA_CONF is amended to point to the test dir
        env['JVM_OPTS'] = os.environ.get('JVM_OPTS', '') \
            + '-Dcassandra.config=file:////{} '.format(os.path.join(env['CASSANDRA_CONF'], CASSANDRA_CONF))

    return env


def make_dse_env(install_dir, node_path, node_ip):
    version = get_version_from_build(node_path=node_path)
    env = os.environ.copy()
    env['MAX_HEAP_SIZE'] = os.environ.get('CCM_MAX_HEAP_SIZE', '500M')
    env['HEAP_NEWSIZE'] = os.environ.get('CCM_HEAP_NEWSIZE', '50M')
    env['MAX_DIRECT_MEMORY'] = os.environ.get('CCM_MAX_DIRECT_SIZE', '2048M') # no effect before 6.0
    if version < '6.0':
        env['SPARK_WORKER_MEMORY'] = os.environ.get('SPARK_WORKER_MEMORY', '1024M')
        env['SPARK_WORKER_CORES'] = os.environ.get('SPARK_WORKER_CORES', '2')
    else:
        env['ALWAYSON_SQL_LOG_DIR'] = os.path.join(node_path, 'logs')
    env['DSE_HOME'] = os.path.join(install_dir)
    env['DSE_CONF'] = os.path.join(node_path, 'resources', 'dse', 'conf')
    env['CASSANDRA_HOME'] = os.path.join(install_dir, 'resources', 'cassandra')
    env['CASSANDRA_CONF'] = os.path.join(node_path, 'resources', 'cassandra', 'conf')
    env['HIVE_CONF_DIR'] = os.path.join(node_path, 'resources', 'hive', 'conf')
    env['SQOOP_CONF_DIR'] = os.path.join(node_path, 'resources', 'sqoop', 'conf')
    env['TOMCAT_HOME'] = os.path.join(node_path, 'resources', 'tomcat')
    env['TOMCAT_CONF_DIR'] = os.path.join(node_path, 'resources', 'tomcat', 'conf')
    env['PIG_CONF_DIR'] = os.path.join(node_path, 'resources', 'pig', 'conf')
    env['MAHOUT_CONF_DIR'] = os.path.join(node_path, 'resources', 'mahout', 'conf')
    env['SPARK_CONF_DIR'] = os.path.join(node_path, 'resources', 'spark', 'conf')
    env['SHARK_CONF_DIR'] = os.path.join(node_path, 'resources', 'shark', 'conf')
    env['GREMLIN_CONSOLE_CONF_DIR'] = os.path.join(node_path, 'resources', 'graph', 'gremlin-console', 'conf')
    env['SPARK_WORKER_DIR'] = os.path.join(node_path, 'spark', 'worker')
    env['SPARK_LOCAL_DIRS'] = os.path.join(node_path, 'spark', '.local')
    env['SPARK_EXECUTOR_DIRS'] = os.path.join(node_path, 'spark', 'rdd')
    env['SPARK_WORKER_LOG_DIR'] = os.path.join(node_path, 'logs', 'spark', 'worker')
    env['SPARK_MASTER_LOG_DIR'] = os.path.join(node_path, 'logs', 'spark', 'master')
    env['DSE_LOG_ROOT'] = os.path.join(node_path, 'logs', 'dse')
    env['CASSANDRA_LOG_DIR'] = os.path.join(node_path, 'logs')
    env['SPARK_LOCAL_IP'] = '' + node_ip
    if version >= '5.0':
        env['HADOOP1_CONF_DIR'] = os.path.join(node_path, 'resources', 'hadoop', 'conf')
        env['HADOOP2_CONF_DIR'] = os.path.join(node_path, 'resources', 'hadoop2-client', 'conf')
    else:
        env['HADOOP_CONF_DIR'] = os.path.join(node_path, 'resources', 'hadoop', 'conf')
    return env


def check_win_requirements():
    if is_win():
        # Make sure ant.bat is in the path and executable before continuing
        try:
            subprocess.Popen('ant.bat', stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        except Exception:
            sys.exit("ERROR!  Could not find or execute ant.bat.  Please fix this before attempting to run ccm on Windows.")

        # Confirm matching architectures
        # 32-bit python distributions will launch 32-bit cmd environments, losing PowerShell execution privileges on a 64-bit system
        if sys.maxsize <= 2 ** 32 and platform.machine().endswith('64'):
            sys.exit("ERROR!  64-bit os and 32-bit python distribution found.  ccm requires matching architectures.")


def is_win():
    return sys.platform in ("cygwin", "win32")


def is_modern_windows_install(version):
    """
    The 2.1 release line was when Cassandra received beta windows support.
    Many features are gated based on that added compatibility.

    Handles floats, strings, and LooseVersions by first converting all three types to a string, then to a LooseVersion.
    """
    version = LooseVersion(str(version))
    if is_win() and version >= LooseVersion('2.1'):
        return True
    else:
        return False


def is_ps_unrestricted():
    if not is_win():
        raise CCMError("Can only check PS Execution Policy on Windows")
    else:
        try:
            p = subprocess.Popen(['powershell', 'Get-ExecutionPolicy'], stdout=subprocess.PIPE)
        # pylint: disable=E0602
        except WindowsError:
            print_("ERROR: Could not find powershell. Is it in your path?")
        if "Unrestricted" in str(p.communicate()[0]):
            return True
        else:
            return False


def join_bin(root, dir, executable):
    return os.path.join(root, dir, platform_binary(executable))


def platform_binary(input):
    return input + ".bat" if is_win() else input


def platform_pager():
    return "more" if sys.platform == "win32" else "less"


def add_exec_permission(path, executable):
    # 1) os.chmod on Windows can't add executable permissions
    # 2) chmod from other folders doesn't work in cygwin, so we have to navigate the shell
    # to the folder with the executable with it and then chmod it from there
    if sys.platform == "cygwin":
        cmd = "cd " + path + "; chmod u+x " + executable
        os.system(cmd)


def parse_path(executable):
    sep = os.sep
    if sys.platform == "win32":
        sep = "\\\\"
    tokens = re.split(sep, executable)
    del tokens[-1]
    return os.sep.join(tokens)


def parse_bin(executable):
    tokens = re.split(os.sep, executable)
    return tokens[-1]


def get_stress_bin(install_dir):
    candidates = [
        os.path.join(install_dir, 'contrib', 'stress', 'bin', 'stress'),
        os.path.join(install_dir, 'tools', 'stress', 'bin', 'stress'),
        os.path.join(install_dir, 'tools', 'bin', 'stress'),
        os.path.join(install_dir, 'tools', 'bin', 'cassandra-stress'),
        os.path.join(install_dir, 'resources', 'cassandra', 'tools', 'bin', 'cassandra-stress')
    ]
    candidates = [platform_binary(s) for s in candidates]

    for candidate in candidates:
        if os.path.exists(candidate):
            stress = candidate
            break
    else:
        raise Exception("Cannot find stress binary (maybe it isn't compiled)")

    # make sure it's executable -> win32 doesn't care
    if sys.platform == "cygwin":
        # Yes, we're unwinding the path join from above.
        path = parse_path(stress)
        short_bin = parse_bin(stress)
        add_exec_permission(path, short_bin)
    elif not os.access(stress, os.X_OK):
        try:
            os.chmod(stress, os.stat(stress).st_mode | stat.S_IXUSR)
        except:
            raise Exception("stress binary is not executable: %s" % (stress,))

    return stress


def isDse(install_dir):
    if install_dir is None:
        raise ArgumentError('Undefined installation directory')

    bin_dir = os.path.join(install_dir, BIN_DIR)

    if not os.path.exists(bin_dir):
        raise ArgumentError('Installation directory does not contain a bin directory: %s' % install_dir)

    dse_script = os.path.join(bin_dir, 'dse')
    return os.path.exists(dse_script)


def isOpscenter(install_dir):
    if install_dir is None:
        raise ArgumentError('Undefined installation directory')

    bin_dir = os.path.join(install_dir, BIN_DIR)

    if not os.path.exists(bin_dir):
        raise ArgumentError('Installation directory does not contain a bin directory')

    opscenter_script = os.path.join(bin_dir, 'opscenter')
    return os.path.exists(opscenter_script)


def validate_install_dir(install_dir):
    if install_dir is None:
        raise ArgumentError('Undefined installation directory')

    # Windows requires absolute pathing on installation dir - abort if specified cygwin style
    if is_win():
        if ':' not in install_dir:
            raise ArgumentError('%s does not appear to be a cassandra or dse installation directory.  Please use absolute pathing (e.g. C:/cassandra.' % install_dir)

    bin_dir = os.path.join(install_dir, BIN_DIR)
    if isDse(install_dir):
        conf_dir = os.path.join(install_dir, DSE_CASSANDRA_CONF_DIR)
    elif isOpscenter(install_dir):
        conf_dir = os.path.join(install_dir, OPSCENTER_CONF_DIR)
    else:
        conf_dir = os.path.join(install_dir, CASSANDRA_CONF_DIR)
    cnd = os.path.exists(bin_dir)
    cnd = cnd and os.path.exists(conf_dir)
    if not isOpscenter(install_dir):
        cnd = cnd and os.path.exists(os.path.join(conf_dir, CASSANDRA_CONF))
    if not cnd:
        raise ArgumentError('%s does not appear to be a cassandra or dse installation directory' % install_dir)


def fix_bdp_dse_db_install_dir(install_dir):
    maybe_dsedb_dir = os.path.join(install_dir, 'dse-db')
    if os.path.exists(maybe_dsedb_dir):
        install_dir = maybe_dsedb_dir
    return install_dir


def assert_socket_available(itf):
    info = socket.getaddrinfo(itf[0], itf[1], socket.AF_UNSPEC, socket.SOCK_STREAM)
    if not info:
        raise UnavailableSocketError("Failed to get address info for [%s]:%s" % itf)

    (family, socktype, proto, canonname, sockaddr) = info[0]
    s = socket.socket(family, socktype)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    try:
        s.bind(sockaddr)
        s.close()
        return True
    except socket.error as msg:
        s.close()
        addr, port = itf
        raise UnavailableSocketError("Inet address %s:%s is not available: %s; a cluster may already be running or you may need to add the loopback alias" % (addr, port, msg))


def check_socket_listening(itf, timeout=60):
    end = time.time() + timeout
    while time.time() <= end:
        try:
            sock = socket.socket()
            sock.connect(itf)
            sock.close()
            return True
        except socket.error:
            if sock:
                sock.close()
            # Try again in another 200ms
            time.sleep(.2)

    return False


def interface_is_ipv6(itf):
    info = socket.getaddrinfo(itf[0], itf[1], socket.AF_UNSPEC, socket.SOCK_STREAM)
    if not info:
        raise UnavailableSocketError("Failed to get address info for [%s]:%s" % itf)

    return socket.AF_INET6 == info[0][0]

# note: does not handle collapsing hextets with leading zeros


def normalize_interface(itf):
    if not itf:
        return itf
    ip = itf[0]
    parts = ip.partition('::')
    if '::' in parts:
        missing_hextets = 9 - ip.count(':')
        zeros = '0'.join([':'] * missing_hextets)
        ip = ''.join(['0' if p == '' else zeros if p == '::' else p for p in ip.partition('::')])
    return (ip, itf[1])


def parse_settings(args, literal_yaml=False):
    settings = {}
    if literal_yaml:
        for s in args:
            settings = dict(settings, **yaml.safe_load(s))
    else:
        for s in args:
            if is_win():
                # Allow for absolute path on Windows for value in key/value pair
                splitted = s.split(':', 1)
            else:
                splitted = s.split(':')
            if len(splitted) != 2:
                raise ArgumentError("A new setting should be of the form 'key: value', got " + s)
            key = splitted[0].strip()
            val = splitted[1].strip()
            # ok, that's not super beautiful
            if val.lower() == "true":
                val = True
            elif val.lower() == "false":
                val = False
            else:
                try:
                    val = int(val)
                except ValueError:
                    pass
            splitted = key.split('.')
            split_length = len(splitted)
            if split_length >= 2:
                # Where we are currently at in the dict.
                tree_pos = settings
                # Iterate over each split and build structure as needed.
                for pos in range(split_length):
                    split = splitted[pos]
                    if pos == split_length - 1:
                        # If at the last split, set value.
                        tree_pos[split] = val
                    else:
                        # If not at last split, create a new dict at the current
                        # position for this split if it doesn't already exist
                        # and update the current position.
                        if split not in tree_pos:
                            tree_pos[split] = {}
                        tree_pos = tree_pos[split]
            else:
                settings[key] = val
    return settings

#
# Copy file from source to destination with reasonable error handling
#


def copy_file(src_file, dst_file):
    try:
        shutil.copy2(src_file, dst_file)
    except (IOError, shutil.Error) as e:
        print_(str(e), file=sys.stderr)
        exit(1)


def copy_directory(src_dir, dst_dir):
    for name in os.listdir(src_dir):
        filename = os.path.join(src_dir, name)
        if os.path.isfile(filename):
            shutil.copy(filename, dst_dir)


def get_cassandra_version_from_build(install_dir=None, node_path=None, cassandra=False):
    return get_version_from_build(install_dir, node_path, cassandra, ignore_dse=True)


def get_version_from_build(install_dir=None, node_path=None, cassandra=False, ignore_dse=False):
    if install_dir is None and node_path is not None:
        install_dir = get_install_dir_from_cluster_conf(node_path)
    build_properties = None
    build_gradle = None
    if install_dir is not None:
        # Binary cassandra installs will have a 0.version.txt file
        version_file = os.path.join(install_dir, '0.version.txt')
        if os.path.exists(version_file):
            with open(version_file) as f:
                return LooseVersion(f.read().strip())

        if not ignore_dse:
            # For DSE look for a dse*.jar and extract the version number
            dse_version = get_dse_version(install_dir)
            if dse_version is not None:
                if cassandra:
                    return get_dse_cassandra_version(install_dir)
                else:
                    return LooseVersion(dse_version)

        # Source cassandra installs we can read from build.xml
        build = os.path.join(install_dir, 'build.xml')
        if os.path.exists(build):
            with open(build) as f:
                for line in f:
                    match = re.search('name="base\.version" value="([0-9.]+)[^"]*"', line)
                    if match:
                        return LooseVersion(match.group(1))

        # Since DB-1701 the "cassandraBaseVersion" is in build.properties - before
        # that number was hard coded in dse-db/build.gradle. Leave that backward
        # compatibility here, it is required for DSE 5.0, 5.1 + 6.0 builds.

        if os.path.basename(os.path.abspath(install_dir)) == 'dse-db':
            # discover version from already fixed install dir
            build_properties = os.path.join(install_dir, '..', 'build.properties')
            build_gradle = os.path.join(install_dir, 'build.gradle')
        else:
            # discover version from cloned repo
            build_properties = os.path.join(install_dir, 'build.properties')
            build_gradle = os.path.join(install_dir, 'dse-db', 'build.gradle')

        # For dse-db clusters from the bdp repo we can read from build.properties
        # (since DSE 6.7)
        if os.path.exists(build_properties):
            with open(build_properties) as f:
                for line in f:
                    match = re.search("cassandraBaseVersion = ([0-9.]+)[^']", line)
                    if match:
                        return LooseVersion(match.group(1))

        # For dse-db clusters from the bdp repo we can read from build.gradle
        # (DSE 5.0, 5.1, 6.0)
        if os.path.exists(build_gradle):
            with open(build_gradle) as f:
                for line in f:
                    match = re.search("version = '([0-9.]+)[^']*", line)
                    if match:
                        return LooseVersion(match.group(1))

    raise CCMError("Cannot find version in install dir {} - build_properties={}  build_gradle={}".format(install_dir, build_properties, build_gradle))


def get_dse_version_from_build(install_dir=None, node_path=None):
    dse_version = get_dse_version_from_build_safe(install_dir=install_dir, node_path=node_path)
    if not dse_version:
        raise Exception("Cannot find DSE version from install_dir=%s node_path=%s" % (install_dir, node_path))
    return dse_version


def get_dse_version_from_build_safe(install_dir=None, node_path=None):
    if install_dir is None and node_path is not None:
        install_dir = get_install_dir_from_cluster_conf(node_path)
    if install_dir is not None:
        # Binary cassandra installs will have a 0.version.txt file
        version_file = os.path.join(install_dir, '0.version.txt')
        if os.path.exists(version_file):
            with open(version_file) as f:
                return LooseVersion(f.read().strip())
        # For DSE look for a dse*.jar and extract the version number
        dse_version = get_dse_version(install_dir)
        if dse_version is not None:
            return LooseVersion(dse_version)
        # Source cassandra installs we can read from build.xml
        build = os.path.join(install_dir, 'build.xml')
        if os.path.exists(build):
            with open(build) as f:
                for line in f:
                    match = re.search('name="base\.dse\.version" value="([0-9.]+)[^"]*"', line)
                    if match:
                        return LooseVersion(match.group(1))
        # if this is a dsedb install from the bdp repo, we just look up the normal dse version
        bdp_dir = None
        # detect if install_dir points to bdp repo root or dse-db subdir
        if os.path.basename(os.path.abspath(install_dir)) == 'dse-db':
            bdp_dir = os.path.join(os.path.abspath(install_dir), os.pardir)
        elif os.path.exists(os.path.join(install_dir, 'dse-db')):
            bdp_dir = install_dir
        if bdp_dir:
            bdp_version_txt = os.path.join(bdp_dir, 'VERSION.txt')
            if os.path.exists(bdp_version_txt):
                with open(bdp_version_txt) as f:
                    return LooseVersion(f.read().strip())
    return None


def get_dse_version(install_dir):
    for root, dirs, files in os.walk(install_dir):
        for file in files:
            match = re.search('^dse(?:-core)?-([0-9.]+)(?:-.*)?\.jar', file)
            if match:
                return match.group(1)
    return None


def get_dse_cassandra_version(install_dir):
    dse_cmd = os.path.join(install_dir, 'bin', 'dse')
    (output, stderr) = subprocess.Popen([dse_cmd, "cassandra", '-v'], stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
    output = output.rstrip()
    match = re.search('([0-9.]+)(?:-.*)?', str(output))
    if match:
        return LooseVersion(match.group(1))

    raise ArgumentError("Unable to determine Cassandra version in: %s.\n\tstdout: '%s'\n\tstderr: '%s'"
      % (install_dir, output, stderr))

def get_install_dir_from_cluster_conf(node_path):
    file = os.path.join(os.path.dirname(node_path), "cluster.conf")
    with open(file) as f:
        for line in f:
            match = re.search('install_dir: (.*?)$', line)
            if match:
                return match.group(1)
    return None


def is_dse_cluster(path):
    try:
        with open(os.path.join(path, 'CURRENT'), 'r') as f:
            name = f.readline().strip()
            cluster_path = os.path.join(path, name)
            filename = os.path.join(cluster_path, 'cluster.conf')
            with open(filename, 'r') as f:
                data = yaml.safe_load(f)
            if 'dse_dir' in data:
                return True
    except IOError:
        return False


def invalidate_cache():
    rmdirs(os.path.join(get_default_path(), 'repository'))


def get_jdk_version_int(process='java'):
    jdk_version = float(get_jdk_version(process))
    # Make it Java 8 instead of 1.8 (or 7 instead of 1.7)
    jdk_version = int(jdk_version if jdk_version >= 2 else 10*(jdk_version-1))
    return jdk_version


def get_jdk_version(process='java'):
    """
    Retrieve the Java version as reported in the quoted string returned
    by invoking 'java -version'.
    Works for Java 1.8, Java 9 and newer.
    """
    try:
        version = subprocess.check_output([process, '-version'], stderr=subprocess.STDOUT)
    except OSError:
        print_("ERROR: Could not find java. Is it in your path?")
        exit(1)

    return _get_jdk_version(version)


def _get_jdk_version(version):
    ver_pattern = '\"(\d+\.\d+).*\"'
    if re.search(ver_pattern, str(version)):
        return re.search(ver_pattern, str(version)).groups()[0]
    # like the output 'java version "9"' for 'java -version'
    ver_pattern = '\"(\d+).*\"'
    return re.search(ver_pattern, str(version)).groups()[0] + ".0"


def update_java_version(jvm_version=None, install_dir=None, dse_version=None, cassandra_version=None, env=None,
                        for_build=False, info_message=None):
    """
    Updates or fixes the Java version (JAVA_HOME environment).
    If 'jvm_version' is explicitly set, that one will be used.
    Otherwise, the Java version will be guessed from the provided 'dse_version' and 'cassandra_version' parameters.
    If the version-parameters are not specified, those will be inquired from the 'install_dir'.

    :param jvm_version: The Java version to use - must be the major Java version number like 8 or 11.
    :param install_dir: Software installation directory.
    :param dse_version: The DSE version to consider for choosing the correct Java version. This one takes
    precedence over 'cassandra_version'.
    :param cassandra_version: The Cassandra version to consider for choosing the correct Java version.
    :param env: Current OS environment variables.
    :param for_build: whether the code should check for a valid version to build or run bdp. Currently only
    applies to source tree that have a 'build-env.yaml' file.
    :param info_message: String logged with info/error messages
    :return: the maybe updated OS environment variables. If 'env' was 'None' and JAVA_HOME needs to be set,
    the result will also contain the current OS environment variables from 'os.environ'.
    """

    env = env if env else os.environ

    # Code to ensure that we start DSE/C* using the correct Java version.
    # This is important especially after DB-468 (Java 11 support) when dtests are run using Java 11
    # but a "manually" configured (set_install_dir()) different version requires Java 8.
    current_jdk_version = float(get_jdk_version('{}/bin/java'.format(env['JAVA_HOME']) if 'JAVA_HOME' in env else 'java'))
    # Make it Java 8 instead of 1.8 (or 7 instead of 1.7)
    current_jdk_version = int(current_jdk_version if current_jdk_version >= 2 else 10*(current_jdk_version-1))

    return _update_java_version(current_jdk_version,
                                jvm_version=jvm_version, install_dir=install_dir, dse_version=dse_version,
                                cassandra_version=cassandra_version, env=env,
                                for_build=for_build, info_message=info_message)


def _update_java_version(current_jdk_version,
                         jvm_version=None, install_dir=None, dse_version=None, cassandra_version=None, env=None,
                         for_build=False, info_message=None):
    # Internal variant accessible for tests

    if env is None:
        raise RuntimeError("env passed to _update_java_version must not be None")

    if dse_version is None and install_dir:
        dse_version = get_dse_version_from_build_safe(install_dir)
    if cassandra_version is None and install_dir:
        cassandra_version = get_version_from_build(install_dir)

    # conservative Java version defaults
    build_versions = [8]
    run_versions = [8]

    if not jvm_version:
        if dse_version:
            # Use the build-env.yaml file introduced with DB-468 to gather the allowed Java versions
            if install_dir:
                build_env_file = os.path.join(install_dir, 'build-env.yaml')
                if not os.path.exists(build_env_file):
                    # if 'install_dir' points into the 'dse-db' folder, try the parent directory
                    build_env_file = os.path.join(install_dir, '..', 'build-env.yaml')
            else:
                build_env_file = None
            if '6.8' <= dse_version < '6.9':
                # DSP-468 - Java 8 + 11 for DSE 6.8
                build_versions = [11, 8]
                run_versions = [8] # DSE 6.8 doesn't officially support Java 11, so don't try to it against Java 11
            elif build_env_file and os.path.exists(build_env_file):
                # build-env.yaml contains an "array" of valid Java versions for tests in 'java_runtime_versions'.
                # Note that we only have that file in the bdp source tree.
                with open(build_env_file, 'r') as stream:
                    build_env = yaml.safe_load(stream)
                    build_versions = build_env['java_build_versions']
                    run_versions = build_env['java_runtime_versions']
            elif dse_version >= '7.0':
                # DSP-20483 - Java 11 only for DSE 7.0/DCE 1.0
                build_versions = [11]
                run_versions = [11, 12, 13, 14, 15]
            # DSE versions 5.0-6.7 use the defaults
            elif dse_version < '5.0':
                build_versions = [8]
                run_versions = [8]  # don't use Java 7
        else:
            if cassandra_version >= '4.0':
                build_versions = [8, 11]
                run_versions = [8, 11, 12, 13]
            # Cassandra versions 3.x use the defaults
            elif cassandra_version < '3.0':
                build_versions = [8]
                run_versions = [8]  # don't use Java 7

        if for_build:
            if current_jdk_version != build_versions[-1]:
                jvm_version = build_versions[-1]
            # Check for the _additional_ JAVAn_HOME variables that need to be set.
            for check_version in build_versions[:-1]:
                check_env = 'JAVA{}_HOME'.format(check_version)
                if check_env not in env:
                    raise RuntimeError("Building DSE {} requires the environment to contain {}, but (at least) {} is missing"
                                       .format(dse_version, ' and '.join(['JAVA{}_HOME'.format(v) for v in build_versions[:-1]]), check_env))
        else:
            if current_jdk_version not in run_versions and len(run_versions) >= 1:
                jvm_version = run_versions[0]

        if jvm_version:
            warning('{}: Java {} is incompatible to DSE {}/Cassandra {}, using Java {} for the current invocation'
                    .format(info_message, current_jdk_version, dse_version, cassandra_version, jvm_version))
    else:
        # Called proved an explicit Java version
        info('{}: Using explicitly requested Java version {} for the current invocation of DSE {}/Cassandra {}'
             .format(info_message, jvm_version, dse_version, cassandra_version))

    if jvm_version:
        new_java_home = 'JAVA{}_HOME'.format(jvm_version)
        if new_java_home not in env:
            if for_build:
                raise RuntimeError("You need to set {} to build DSE {}/Cassandra {}"
                                   .format(' and '.join(['JAVA{}_HOME'.format(v) for v in build_versions]),
                                           dse_version, cassandra_version))
            else:
                raise RuntimeError("You need to set {} to run DSE {}/Cassandra {}"
                                   .format(' or '.join(['JAVA{}_HOME'.format(v) for v in run_versions]),
                                           dse_version, cassandra_version))
        env['JAVA_HOME'] = env[new_java_home]
        env['PATH'] = '{}/bin:{}'.format(env[new_java_home], env['PATH'] if 'PATH' in env else '')
    return env


def assert_jdk_valid_for_cassandra_version(cassandra_version):
    jdk_version = float(get_jdk_version())
    if cassandra_version >= '4.0':
        if jdk_version < 1.8 or 9 <= jdk_version < 11:
            error('Cassandra {} requires Java 1.8 or Java 11, found Java {}'.format(cassandra_version, jdk_version))
            exit(1)
    elif cassandra_version >= '3.0' and jdk_version != 1.8:
        error('Cassandra {} requires Java 1.8, found Java {}'.format(cassandra_version, jdk_version))
        exit(1)
    elif cassandra_version < '3.0' and (jdk_version < 1.7 or jdk_version > 1.8):
        error('Cassandra {} requires Java 1.7 or 1.8, found Java {}'.format(cassandra_version, jdk_version))
        exit(1)


def merge_configuration(original, changes, delete_empty=True, delete_always=False):
    if not isinstance(original, dict):
        # if original is not a dictionary, assume changes override it.
        new = changes
    else:
        # Copy original so we do not mutate it.
        new = copy.deepcopy(original)
        for k, v in changes.items():
            # If the new value is None or an empty string, delete it
            # if it's in the original data.
            if delete_empty and k in new and new[k] is not None and \
                    (v is None or (isinstance(v, str) and len(v) == 0)):
                del new[k]
            elif not delete_always:
                new_value = v
                # If key is in both dicts, update it with new values.
                if k in new:
                    if isinstance(v, dict):
                        new_value = merge_configuration(new[k], v, delete_empty)
                new[k] = new_value
    return new


def is_int_not_bool(obj):
    return isinstance(obj, six.integer_types) and not isinstance(obj, bool)


def is_intlike(obj):
    return isinstance(obj, six.integer_types)


def wait_for_any_log(nodes, pattern, timeout, filename='system.log', marks=None):
    """
    Look for a pattern in the system.log of any in a given list
    of nodes.
    @param nodes The list of nodes whose logs to scan
    @param pattern The target pattern
    @param timeout How long to wait for the pattern. Note that
                    strictly speaking, timeout is not really a timeout,
                    but a maximum number of attempts. This implies that
                    the all the grepping takes no time at all, so it is
                    somewhat inaccurate, but probably close enough.
    @param marks A dict of nodes to marks in the file. Keys must match the first param list.
    @return The first node in whose log the pattern was found
    """
    if marks is None:
        marks = {}
    for _ in range(timeout):
        for node in nodes:
            found = node.grep_log(pattern, filename=filename, from_mark=marks.get(node, None))
            if found:
                return node
        time.sleep(1)

    raise TimeoutError(time.strftime("%d %b %Y %H:%M:%S", time.gmtime()) +
                       " Unable to find: " + repr(pattern) + " in any node log within " + str(timeout) + "s")


def get_default_signals():
    if is_win():
        # Fill the dictionary with SIGTERM as the cluster is killed forcefully
        # on Windows regardless of assigned signal (TASKKILL is used)
        default_signal_events = {'1': signal.SIGTERM, '9': signal.SIGTERM}
    else:
        default_signal_events = {'1': signal.SIGHUP, '9': signal.SIGKILL}
    return default_signal_events


def watch_log_for(log_file, exprs, filename, name, from_mark=None, timeout=600,
                  is_running=lambda: True):
    start = time.time()
    tofind = [exprs] if isinstance(exprs, string_types) else exprs
    tofind = [re.compile(e) for e in tofind]
    matchings = []
    reads = ""
    if len(tofind) == 0:
        return None

    output_read = False
    while not os.path.exists(log_file):
        time.sleep(.5)
        if start + timeout < time.time():
            raise TimeoutError(time.strftime("%d %b %Y %H:%M:%S", time.gmtime()) + " [" + name + "] Timed out waiting for {} to be created.".format(log_file))

    start = time.time()  # Reset start time after log file appeared (since it may eat several seconds)
    f = None
    try:
        f = open(log_file)

        # If 'from_mark' has been specified and the file size is smaller, then the log file has
        # been rotated and the expected log message might have been missed. Log a warning in that case.
        if not from_mark:
            from_mark = 0
        else:
            file_size = os.path.getsize(log_file)
            if file_size < from_mark:
                warning("Detected rotation of {} log file {}. "
                        "Some log entries might be missed and cause the function to fail. "
                        "from_mark = {}, file size = {}"
                        .format(name, filename, from_mark, file_size))
                from_mark = 0
            f.seek(from_mark)

        while True:
            # Python 2.7 on OSX fails to recognize newly added lines to the file we're watching here.
            # This seek/tell combination, as silly as it may look, works around that issue on OSX.
            # See DB-2820 for a code sample.
            f.seek(f.tell())

            # Read and check as much as we can read at once.
            # I.e. prevent checking the process status after every line.
            lines = f.readlines()

            # If there are no more lines in the log and the process is not running,
            # then we timeout early
            if not lines and not is_running():
                raise TimeoutError(time.strftime("%d %b %Y %H:%M:%S", time.gmtime()) + " [" + name + "] Process has finished but pattern is still missing: " +
                                   str([e.pattern for e in tofind]) + ":\n" + reads[:50] + ".....\nSee {} for remainder".format(filename))

            for line in lines:
                if line and line.endswith('\n'):  # Do not consider incomplete lines (without a trailing newline)
                    from_mark = f.tell()
                    reads = reads + line
                    for e in tofind:
                        m = e.search(line)
                        if m:
                            matchings.append((line, m))
                            tofind.remove(e)
                            if len(tofind) == 0:
                                return matchings[0] if isinstance(exprs, string_types) else matchings
                else:
                    break

            if start + timeout < time.time():
                raise TimeoutError(time.strftime("%d %b %Y %H:%M:%S", time.gmtime()) + " [" + name + "] Missing: " + str([e.pattern for e in tofind]) + ":\n" + reads[:50] + ".....\nSee {} for remainder".format(filename))

            # Yep, it's ugly. Don't sleep long to (hopefully) not miss a message in a rotated log file.
            time.sleep(.2)

            # Try to detect log file rotation. If the size of the file via os.path.getsize() is
            # smaller than the 'from_mark', a log file rotation happened. Since we only get here,
            # when there is no more data to read from the previously opened file handle, simply
            # close the old file handle and reopen the file.
            file_size = os.path.getsize(log_file)
            if file_size < from_mark:
                warning("Detected rotation of {} log file {}".format(name, filename))
                f.close()
                f = open(log_file)
                from_mark = 0
                continue
    finally:
        if f:
            f.close()


def set_logback_no_rotation(logback_config_file):
    if os.path.exists(logback_config_file):
        with open(logback_config_file) as src:
            logback_config = "".join(src.readlines())

        logback_config = re.sub(r'ch\.qos\.logback\.core\.rolling\.RollingFileAppender',
                                'ch.qos.logback.core.FileAppender',
                                logback_config)

        # A non-greedy variant of that regex would be nicer than the loop
        #   regex_total_size = r'<rollingPolicy .*</rollingPolicy>'
        #   logback_config = re.sub(regex_total_size, '', logback_config, flags=re.DOTALL + re.MULTILINE)
        while True:
            i = logback_config.find('<rollingPolicy')
            e = logback_config.find('</rollingPolicy>')
            if i < 0 or e < 0:
                break
            logback_config = logback_config[0:i] + logback_config[e + len('</rollingPolicy>'):]

        with open(logback_config_file, 'w') as dst:
            dst.write(logback_config)
