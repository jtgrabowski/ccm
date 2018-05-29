import os
import re
import unittest
import shutil
import tempfile
import time
import threading
from distutils.version import LooseVersion

from mock import patch

from ccmlib import common
from . import ccmtest


class TestCommon(ccmtest.Tester):

    def test_normalize_interface(self):
        normalized = common.normalize_interface(('::1', 9042))
        self.assertEqual(normalized, ('0:0:0:0:0:0:0:1', 9042))

        normalized = common.normalize_interface(('127.0.0.1', 9042))
        self.assertEqual(normalized, ('127.0.0.1', 9042))

        normalized = common.normalize_interface(('fe80::3e15:c2ff:fed3:db74%en0', 9042))
        self.assertEqual(normalized, ('fe80:0:0:0:3e15:c2ff:fed3:db74%en0', 9042))

        normalized = common.normalize_interface(('fe80::1%lo0', 9042))
        self.assertEqual(normalized, ('fe80:0:0:0:0:0:0:1%lo0', 9042))

        normalized = common.normalize_interface(('fd6d:404d:54cb::1', 9042))
        self.assertEqual(normalized, ('fd6d:404d:54cb:0:0:0:0:1', 9042))

    @patch('ccmlib.common.is_win')
    def test_is_modern_windows_install(self, mock_is_win):
        mock_is_win.return_value = True
        self.assertTrue(common.is_modern_windows_install(2.1))
        self.assertTrue(common.is_modern_windows_install('2.1'))
        self.assertTrue(common.is_modern_windows_install(LooseVersion('2.1')))

        self.assertTrue(common.is_modern_windows_install(3.12))
        self.assertTrue(common.is_modern_windows_install('3.12'))
        self.assertTrue(common.is_modern_windows_install(LooseVersion('3.12')))

        self.assertFalse(common.is_modern_windows_install(1.0))
        self.assertFalse(common.is_modern_windows_install('1.0'))
        self.assertFalse(common.is_modern_windows_install(LooseVersion('1.0')))

    def test_merge_configuration(self):
        # test for merging dict val in key, value pair
        dict0 = dict1 = {'key': {'val1': True}}
        dict2 = {'key': {'val2': False}}

        for k, v in dict2.items():
            dict0[k].update(v)

        self.assertEqual(common.merge_configuration(dict1, dict2), dict0)

        # test for merging str val in key, value pair
        dict0 = dict1 = {'key': 'val1'}
        dict2 = {'key': 'val2'}

        for k, v in dict2.items():
            dict0[k] = v

        self.assertEqual(common.merge_configuration(dict1, dict2), dict0)

    def test_get_jdk_version(self):
        v8u152 = """java version "1.8.0_152"
                 Java(TM) SE Runtime Environment (build 1.8.0_152-b16)
                 Java HotSpot(TM) 64-Bit Server VM (build 25.152-b16, mixed mode)
                 """
        # Since Java 9, the version string syntax changed.
        # Most relevant change is that trailing .0's are omitted. I.e. Java "9.0.0"
        # version string is not "9.0.0" but just "9".
        v900 = """java version "9"
               Java(TM) SE Runtime Environment (build 9+1)
               Java HotSpot(TM) 64-Bit Server VM (build 9+1, mixed mode)
               """
        v901 = """java version "9.0.1"
               Java(TM) SE Runtime Environment (build 9.0.1+11)
               Java HotSpot(TM) 64-Bit Server VM (build 9.0.1+11, mixed mode)
               """
        # 10-internal, just to have an internal (local) build in here
        v10_int = """openjdk version "10-internal"
                  OpenJDK Runtime Environment (build 10-internal+0-adhoc.jenkins.openjdk-shenandoah-jdk10-release)
                  OpenJDK 64-Bit Server VM (build 10-internal+0-adhoc.jenkins.openjdk-shenandoah-jdk10-release, mixed mode)
                  """
        v1000 = """java version "10"
                Java(TM) SE Runtime Environment (build 9+1)
                Java HotSpot(TM) 64-Bit Server VM (build 9+1, mixed mode)
                """
        v1001 = """java version "10.0.1"
                Java(TM) SE Runtime Environment (build 10.0.1+11)
                Java HotSpot(TM) 64-Bit Server VM (build 10.0.1+11, mixed mode)
                """

        self.assertEqual(common._get_jdk_version(v8u152), "1.8")
        self.assertEqual(common._get_jdk_version(v900), "9.0")
        self.assertEqual(common._get_jdk_version(v901), "9.0")
        self.assertEqual(common._get_jdk_version(v10_int), "10.0")
        self.assertEqual(common._get_jdk_version(v1000), "10.0")
        self.assertEqual(common._get_jdk_version(v1001), "10.0")

    def test_update_java_version(self):
        dummy_env = {'foo': 'bar'}
        env_with_8 = {'JAVA8_HOME': 'some_path_to_8'}
        env_with_8_and_11 = {'JAVA8_HOME': 'some_path_to_8',
                             'JAVA11_HOME': 'some_path_to_11'}
        env_with_11 = {'JAVA11_HOME': 'some_path_to_11'}

        yaml_prefix = os.path.join(self._test_resources(), 'build-envs')

        # Current Java version is fine for DSE 5.0.x
        result = common._update_java_version(current_jdk_version=8, jvm_version=None, install_dir=None,
                                             dse_version=LooseVersion(vstring='5.0'), cassandra_version='3.0.101',
                                             env=dummy_env.copy(),
                                             for_build=False, info_message='test_update_java_version')
        self.assertDictEqual(result, dummy_env.copy())

        # Current Java version is fine for DSE 5.0.x
        result = common._update_java_version(current_jdk_version=8, jvm_version=None, install_dir=None,
                                             dse_version=LooseVersion(vstring='5.0'), cassandra_version='3.0.101',
                                             env=dummy_env.copy(),
                                             for_build=False, info_message='test_update_java_version')
        self.assertDictEqual(result, dummy_env.copy())

        # Java 11 is not ok for DSE 5.0
        with self.assertRaises(RuntimeError) as re:
            common._update_java_version(current_jdk_version=11, jvm_version=None, install_dir=None,
                                        dse_version=LooseVersion(vstring='5.0'), cassandra_version='3.0.102',
                                        env=dummy_env.copy(),
                                        for_build=False, info_message='test_update_java_version')
        self.assertRegexpMatches(str(re.exception),
                                 'You need to set JAVA8_HOME to run DSE 5.0/Cassandra 3.0.102')
        # ... unless it can be re-configured to Java 8
        result = common._update_java_version(current_jdk_version=11, jvm_version=None, install_dir=None,
                                             dse_version=LooseVersion(vstring='5.0'), cassandra_version='3.0.103',
                                             env=env_with_8.copy(),
                                             for_build=False, info_message='test_update_java_version')
        self.assertDictEqual(result, self._env_with_java_version(env_with_8, 8))
        # ... also for builds from source
        result = common._update_java_version(current_jdk_version=11, jvm_version=None, install_dir=None,
                                             dse_version=LooseVersion(vstring='5.0'), cassandra_version='3.0.104',
                                             env=env_with_8.copy(),
                                             for_build=True, info_message='test_update_java_version')
        self.assertDictEqual(result, self._env_with_java_version(env_with_8, 8))

        # Java 42 is not ok for DSE 6.8
        with self.assertRaises(RuntimeError) as re:
            common._update_java_version(current_jdk_version=42, jvm_version=None, install_dir=None,
                                        dse_version=LooseVersion(vstring='7.0'), cassandra_version='4.0.105',
                                        env=dummy_env.copy(),
                                        for_build=False, info_message='test_update_java_version')
        self.assertRegexpMatches(str(re.exception),
                                 'You need to set JAVA11_HOME or JAVA12_HOME or JAVA13_HOME or JAVA14_HOME or JAVA15_HOME to run DSE 7.0/Cassandra 4.0.105')
        # ... unless it can be re-configured to Java 8
        result = common._update_java_version(current_jdk_version=42, jvm_version=None, install_dir=None,
                                             dse_version=LooseVersion(vstring='6.8'), cassandra_version='4.0.106',
                                             env=env_with_8.copy(),
                                             for_build=False, info_message='test_update_java_version')
        self.assertDictEqual(result, self._env_with_java_version(env_with_8, 8))
        # ... also for builds from source with Java 8+11
        with self.assertRaises(RuntimeError) as re:
            common._update_java_version(current_jdk_version=42, jvm_version=None, install_dir=None,
                                        dse_version=LooseVersion(vstring='6.8'), cassandra_version='4.0.107',
                                        env=env_with_8.copy(),
                                        for_build=True, info_message='test_update_java_version')
        self.assertRegexpMatches(str(re.exception),
                                 'Building DSE 6.8 requires the environment to contain JAVA11_HOME, but \(at least\) JAVA11_HOME is missing')
        result = common._update_java_version(current_jdk_version=42, jvm_version=None, install_dir=None,
                                             dse_version=LooseVersion(vstring='6.8'), cassandra_version='4.0.107',
                                             env=env_with_8_and_11.copy(),
                                             for_build=True, info_message='test_update_java_version')
        self.assertDictEqual(result, self._env_with_java_version(env_with_8_and_11, 8))

        # tests with build-env.yaml

        # Java 42 is not ok for DSE 6.8
        with self.assertRaises(RuntimeError) as re:
            common._update_java_version(current_jdk_version=42, jvm_version=None, install_dir=os.path.join(yaml_prefix, '1'),
                                        dse_version=LooseVersion(vstring='6.8'), cassandra_version='4.0.105',
                                        env=dummy_env.copy(),
                                        for_build=False, info_message='test_update_java_version')
        self.assertRegexpMatches(str(re.exception),
                                 'You need to set JAVA8_HOME to run DSE 6.8/Cassandra 4.0.105')
        # ... unless it can be re-configured to Java 8
        result = common._update_java_version(current_jdk_version=42, jvm_version=None, install_dir=os.path.join(yaml_prefix, '1'),
                                             dse_version=LooseVersion(vstring='6.8'), cassandra_version='4.0.106',
                                             env=env_with_8.copy(),
                                             for_build=False, info_message='test_update_java_version')
        self.assertDictEqual(result, self._env_with_java_version(env_with_8, 8))
        # ... also for builds from source with Java 8+11
        with self.assertRaises(RuntimeError) as re:
            common._update_java_version(current_jdk_version=42, jvm_version=None, install_dir=os.path.join(yaml_prefix, '1'),
                                        dse_version=LooseVersion(vstring='6.8'), cassandra_version='4.0.107',
                                        env=env_with_8.copy(),
                                        for_build=True, info_message='test_update_java_version')
        self.assertRegexpMatches(str(re.exception),
                                 'Building DSE 6.8 requires the environment to contain JAVA11_HOME, but \(at least\) JAVA11_HOME is missing')
        result = common._update_java_version(current_jdk_version=42, jvm_version=None, install_dir=os.path.join(yaml_prefix, '1'),
                                             dse_version=LooseVersion(vstring='6.8'), cassandra_version='4.0.107',
                                             env=env_with_8_and_11.copy(),
                                             for_build=True, info_message='test_update_java_version')
        self.assertDictEqual(result, self._env_with_java_version(env_with_8_and_11, 8))

        # An imaginary DSE version based on Java 11 (no support for Java 8)

        # Java 42 is not ok for DSE 9.5
        with self.assertRaises(RuntimeError) as re:
            common._update_java_version(current_jdk_version=42, jvm_version=None, install_dir=os.path.join(yaml_prefix, '2'),
                                        dse_version=LooseVersion(vstring='9.5'), cassandra_version='5.2.108',
                                        env=dummy_env.copy(),
                                        for_build=False, info_message='test_update_java_version')
        self.assertRegexpMatches(str(re.exception),
                                 'You need to set JAVA11_HOME or JAVA12_HOME or JAVA13_HOME to run DSE 9.5/Cassandra 5.2.108')
        # ... unless it can be re-configured to Java 11 (the default)
        result = common._update_java_version(current_jdk_version=42, jvm_version=None, install_dir=os.path.join(yaml_prefix, '2'),
                                             dse_version=LooseVersion(vstring='9.5'), cassandra_version='5.2.109',
                                             env=env_with_11.copy(),
                                             for_build=False, info_message='test_update_java_version')
        self.assertDictEqual(result, self._env_with_java_version(env_with_11, 11))
        # ... also for builds from source with Java 8+11
        with self.assertRaises(RuntimeError) as re:
            common._update_java_version(current_jdk_version=42, jvm_version=None, install_dir=os.path.join(yaml_prefix, '2'),
                                        dse_version=LooseVersion(vstring='9.5'), cassandra_version='5.2.110',
                                        env=env_with_8.copy(),
                                        for_build=True, info_message='test_update_java_version')
        self.assertRegexpMatches(str(re.exception),
                                 'You need to set JAVA11_HOME to build DSE 9.5/Cassandra 5.2.110')
        result = common._update_java_version(current_jdk_version=42, jvm_version=None, install_dir=os.path.join(yaml_prefix, '2'),
                                             dse_version=LooseVersion(vstring='9.5'), cassandra_version='5.2.111',
                                             env=env_with_8_and_11.copy(),
                                             for_build=True, info_message='test_update_java_version')
        self.assertDictEqual(result, self._env_with_java_version(env_with_8_and_11, 11))

    @staticmethod
    def _env_with_java_version(env, version):
        e = env.copy()
        e['JAVA_HOME'] = env['JAVA{}_HOME'.format(version)]
        e['PATH'] = '{}/bin:'.format(env['JAVA{}_HOME'.format(version)])
        return e

    def test_watch_log_for_timeout(self):
        logfile = tempfile.mktemp()
        try:
            with open(logfile, "w") as f:
                f.write("nope nope nope\n")
                f.write("foo bar baz\n")
                f.write("dog tail\n")
                f.write("incomplete line")

            # Sanity checks
            self.assertTrue(os.path.exists(logfile))
            common.watch_log_for(logfile, exprs="bar",
                                 from_mark=0,
                                 filename="a_file", name="test_watch_log_for_timeout",
                                 timeout=1)
            common.watch_log_for(logfile, exprs="foo bar",
                                 from_mark=0,
                                 filename="a_file", name="test_watch_log_for_timeout",
                                 timeout=1)

            # test that it raises a TimeoutError for some expression that does not exist
            self.assertRaises(common.TimeoutError,
                              lambda: common.watch_log_for(logfile, exprs="something that never appears in the log file",
                                                           filename="a_file", name="test_watch_log_for_timeout",
                                                           timeout=0.4))

            # test that it raises a TimeoutError for some expression that is part of an incomplete line
            self.assertRaises(common.TimeoutError,
                              lambda: common.watch_log_for(logfile, exprs="incomplete line",
                                                           filename="a_file", name="test_watch_log_for_timeout",
                                                           timeout=0.4))

            # Test that 'from_mark' works
            size = os.path.getsize(logfile)
            self.assertRaises(common.TimeoutError,
                              lambda: common.watch_log_for(logfile, exprs="foo bar",
                                                           from_mark=size,
                                                           filename="a_file", name="test_watch_log_for_timeout",
                                                           timeout=0.4))

            # Timing sensitive test...
            #
            # Validate that some expression that is written while watch_log_for is running is detected
            def write_after_some_time():
                time.sleep(0.5)
                with open(logfile, "w") as f2:
                    f2.write("\nafter one second\n")
            threading.Thread(target=write_after_some_time).start()

            common.watch_log_for(logfile, exprs="after one second",
                                 from_mark=0,
                                 filename="a_file", name="test_watch_log_for_timeout",
                                 timeout=1)

        finally:
            if os.path.exists(logfile):
                os.unlink(logfile)

    def test_watch_log_works_with_rotation(self):
        logfile = tempfile.mktemp()
        try:
            with open(logfile, "w") as f:
                f.write("nope nope nope\n")
                f.write("foo bar baz\n")
                f.write("dog tail\n")
                f.write("incomplete line")

            # sanity check
            self.assertTrue(os.path.exists(logfile))
            common.watch_log_for(logfile, exprs="foo bar",
                                 from_mark=0,
                                 filename="a_file", name="test_watch_log_works_with_rotation",
                                 timeout=1)

            def rotate_after_some_time():
                time.sleep(0.5)
                os.unlink(logfile)
                with open(logfile, "w") as r:
                    r.write("rotated log stuff\n")
            threading.Thread(target=rotate_after_some_time).start()

            common.watch_log_for(logfile, exprs="rotated log stuff",
                                 from_mark=0,
                                 filename="a_file", name="test_watch_log_works_with_rotation",
                                 timeout=2)

        finally:
            if os.path.exists(logfile):
                os.unlink(logfile)

    def test_set_logback_no_rotation(self):
        logback_source = os.path.join(self._test_resources(), 'logback-rolling-time.xml')
        logback_expect = os.path.join(self._test_resources(), 'logback-rolling-time-expect.xml')
        logback_tmp = tempfile.mktemp()
        with open(logback_source) as src:
            with open(logback_tmp, "w") as dst:
                for line in src.readlines():
                    dst.write(line)
        common.set_logback_no_rotation(logback_tmp)
        with open(logback_expect) as expect:
            with open(logback_tmp) as current:
                expected = expect.readlines()
                updated = current.readlines()
                self.maxDiff = None
                self.assertListEqual(expected, updated)

    def test_substitute_in_list(self):
        input_logdir = """
                        mkdir -p ${CASSANDRA_LOG_DIR}
                        JVM_OPTS="$JVM_OPTS -Xlog:gc*=info,safepoint*=info:file=${CASSANDRA_LOG_DIR}/gc.log:time,uptimenanos,tags,pid,tid,level:filecount=10,filesize=25M"
                        JVM_OPTS="$JVM_OPTS -Xlog:container*=info,logging*=info,os*=info,pagesize*=info,setting*=info,startuptime*=info,system*=info,os+thread=off:file=${CASSANDRA_LOG_DIR}/jvm.log:time,uptimenanos,tags,pid,tid,level:filecount=10,filesize=10M"
                        #JVM_OPTS="$JVM_OPTS -Xlog:inlining*=debug,jit*=debug,monitorinflation*=debug:file=${CASSANDRA_LOG_DIR}/jit.log:time,uptimenanos,tags,pid,tid,level:filecount=10,filesize=10M"

                        mkdir -p ${CASSANDRA_LOG_DIR}
                        JVM_OPTS="$JVM_OPTS -Xloggc:${CASSANDRA_LOG_DIR}/gc.log"
            """.split('\n')
        expect_logdir = """
                        mkdir -p TEST_SUBSTITUTION_LOGDIR
                        JVM_OPTS="$JVM_OPTS -Xlog:gc*=info,safepoint*=info:file=TEST_SUBSTITUTION_LOGDIR/gc.log:time,uptimenanos,tags,pid,tid,level:filecount=10,filesize=25M"
                        JVM_OPTS="$JVM_OPTS -Xlog:container*=info,logging*=info,os*=info,pagesize*=info,setting*=info,startuptime*=info,system*=info,os+thread=off:file=TEST_SUBSTITUTION_LOGDIR/jvm.log:time,uptimenanos,tags,pid,tid,level:filecount=10,filesize=10M"
                        #JVM_OPTS="$JVM_OPTS -Xlog:inlining*=debug,jit*=debug,monitorinflation*=debug:file=TEST_SUBSTITUTION_LOGDIR/jit.log:time,uptimenanos,tags,pid,tid,level:filecount=10,filesize=10M"

                        mkdir -p TEST_SUBSTITUTION_LOGDIR
                        JVM_OPTS="$JVM_OPTS -Xloggc:TEST_SUBSTITUTION_LOGDIR/gc.log"
            """.split('\n')

        input_home = """
                        mkdir -p ${CASSANDRA_HOME}/logs
                        JVM_OPTS="$JVM_OPTS -Xlog:gc*=info,safepoint*=info:file=${CASSANDRA_HOME}/logs/gc.log:time,uptimenanos,tags,pid,tid,level:filecount=10,filesize=25M"
                        JVM_OPTS="$JVM_OPTS -Xlog:container*=info,logging*=info,os*=info,pagesize*=info,setting*=info,startuptime*=info,system*=info,os+thread=off:file=${CASSANDRA_HOME}/logs/jvm.log:time,uptimenanos,tags,pid,tid,level:filecount=10,filesize=10M"
                        #JVM_OPTS="$JVM_OPTS -Xlog:inlining*=debug,jit*=debug,monitorinflation*=debug:file=${CASSANDRA_HOME}/logs/jit.log:time,uptimenanos,tags,pid,tid,level:filecount=10,filesize=10M"

                        mkdir -p ${CASSANDRA_HOME}/logs
                        JVM_OPTS="$JVM_OPTS -Xloggc:${CASSANDRA_HOME}/logs/gc.log"
            """.split('\n')
        expect_home = """
                        mkdir -p TEST_SUBSTITUTION_HOME
                        JVM_OPTS="$JVM_OPTS -Xlog:gc*=info,safepoint*=info:file=TEST_SUBSTITUTION_HOME/gc.log:time,uptimenanos,tags,pid,tid,level:filecount=10,filesize=25M"
                        JVM_OPTS="$JVM_OPTS -Xlog:container*=info,logging*=info,os*=info,pagesize*=info,setting*=info,startuptime*=info,system*=info,os+thread=off:file=TEST_SUBSTITUTION_HOME/jvm.log:time,uptimenanos,tags,pid,tid,level:filecount=10,filesize=10M"
                        #JVM_OPTS="$JVM_OPTS -Xlog:inlining*=debug,jit*=debug,monitorinflation*=debug:file=TEST_SUBSTITUTION_HOME/jit.log:time,uptimenanos,tags,pid,tid,level:filecount=10,filesize=10M"

                        mkdir -p TEST_SUBSTITUTION_HOME
                        JVM_OPTS="$JVM_OPTS -Xloggc:TEST_SUBSTITUTION_HOME/gc.log"
            """.split('\n')

        lines = []
        common._substitute_in_list(input_home,
                                   [[re.compile('[$][{]CASSANDRA_HOME[}]/logs'), 'TEST_SUBSTITUTION_HOME'],
                                    [re.compile('[$][{]CASSANDRA_LOG_DIR[}]'), 'TEST_SUBSTITUTION_LOGDIR']],
                                   lambda l: lines.append(l))
        self.assertListEqual(expect_home, lines)

        lines = []
        common._substitute_in_list(input_logdir,
                                   [[re.compile('[$][{]CASSANDRA_HOME[}]/logs'), 'TEST_SUBSTITUTION_HOME'],
                                    [re.compile('[$][{]CASSANDRA_LOG_DIR[}]'), 'TEST_SUBSTITUTION_LOGDIR']],
                                   lambda l: lines.append(l))
        self.assertListEqual(expect_logdir, lines)

        lines = []
        common._substitute_in_list(input_home + input_logdir,
                                   [[re.compile('[$][{]CASSANDRA_HOME[}]/logs'), 'TEST_SUBSTITUTION_HOME'],
                                    [re.compile('[$][{]CASSANDRA_LOG_DIR[}]'), 'TEST_SUBSTITUTION_LOGDIR']],
                                   lambda l: lines.append(l))
        self.assertListEqual(expect_home + expect_logdir, lines)

        lines = []
        common._substitute_in_list(['file=${CASSANDRA_HOME}/logs/gc.log  file=${CASSANDRA_HOME}/logs/foo.log',
                                    'file=${CASSANDRA_LOG_DIR}/gc.log  file=${CASSANDRA_LOG_DIR}/foo.log',
                                    'file=${CASSANDRA_LOG_DIR}/gc.log  file=${CASSANDRA_HOME}/logs/foo.log',
                                    '${CASSANDRA_LOG_DIR}',
                                    '${CASSANDRA_HOME}/logs',
                                    '${CASSANDRA_LOG_DIR}${CASSANDRA_HOME}/logs'],
                                   [[re.compile('[$][{]CASSANDRA_HOME[}]/logs'), 'TEST_SUBSTITUTION_HOME'],
                                    [re.compile('[$][{]CASSANDRA_LOG_DIR[}]'), 'TEST_SUBSTITUTION_LOGDIR']],
                                   lambda l: lines.append(l))
        self.assertListEqual(['file=TEST_SUBSTITUTION_HOME/gc.log  file=TEST_SUBSTITUTION_HOME/foo.log',
                              'file=TEST_SUBSTITUTION_LOGDIR/gc.log  file=TEST_SUBSTITUTION_LOGDIR/foo.log',
                              'file=TEST_SUBSTITUTION_LOGDIR/gc.log  file=TEST_SUBSTITUTION_HOME/foo.log',
                              'TEST_SUBSTITUTION_LOGDIR',
                              'TEST_SUBSTITUTION_HOME',
                              'TEST_SUBSTITUTION_LOGDIRTEST_SUBSTITUTION_HOME'], lines)

    def test_substitute_in_file(self):
        def _temp_resource(filename):
            tmpfile = tempfile.mktemp()
            shutil.copyfile(os.path.join(self._test_resources(), 'replace', filename), tmpfile)
            return tmpfile

        def _read_file(filename):
            with open(filename) as f:
                return f.read().splitlines()

        def _read_resource(filename):
            return _read_file(os.path.join(self._test_resources(), 'replace', filename))

        # test with a multiple input files and multiple substitutions

        input_c_env = _temp_resource('cassandra-env-input.sh')
        input_jvm_dep = _temp_resource('jvm-dependent-input.sh')

        common.substitute_in_files([input_c_env, input_jvm_dep],
                                   [['[$][{]CASSANDRA_HOME[}]/logs', '/foo/bar/baz/home/log-directory'],
                                    ['[$][{]CASSANDRA_LOG_DIR[}]', '/foo/bar/baz/log-directory']])

        self.assertListEqual(_read_resource('cassandra-env-expect.sh'), _read_file(input_c_env))
        self.assertListEqual(_read_resource('jvm-dependent-expect.sh'), _read_file(input_jvm_dep))

        # test with a single input file and multiple substitutions

        input_c_env = _temp_resource('cassandra-env-input.sh')

        common.substitute_in_files(input_c_env,
                                   [['[$][{]CASSANDRA_HOME[}]/logs', '/foo/bar/baz/home/log-directory'],
                                    ['[$][{]CASSANDRA_LOG_DIR[}]', '/foo/bar/baz/log-directory']])

        self.assertListEqual(_read_resource('cassandra-env-expect.sh'), _read_file(input_c_env))

        # test with a single input file and a single substitution

        input_c_env = _temp_resource('cassandra-env-input.sh')

        common.substitute_in_files(input_c_env,
                                   ['[$][{]CASSANDRA_HOME[}]/logs', '/foo/bar/baz/home/log-directory'])

        self.assertListEqual(_read_resource('cassandra-env-expect2.sh'), _read_file(input_c_env))

    @staticmethod
    def _test_resources():
        if os.path.exists(os.path.join('tests', 'test-resources')):
            return os.path.join('tests', 'test-resources')
        elif os.path.exists('test-resources'):
            return 'test-resources'
        else:
            raise AssertionError('Run this test either from the ccm source tree root directory or in tests/')


if __name__ == '__main__':
    unittest.main()
