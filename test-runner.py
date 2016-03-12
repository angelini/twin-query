#!/usr/bin/env python

import os
import subprocess
import sys


class TestFile:

    def __init__(self, name, db_file, tests):
        self.name = name
        self.db_file = db_file
        self.tests = tests

    def run(self):
        print('Running {}'.format(self.name))
        for test in self.tests:
            self._run_test(test)
        print()

    def _command(self, query):
        return ['target/debug/twin-query', 'query', self.db_file, '{}'.format(query)]

    def _compare_results(self, expected, actual):
        for (e, a) in zip(expected.split('\n'), actual.split('\n')):
            if e.strip() != a.strip():
                return False
        return True

    def _run_test(self, test):
        (query, expected) = test
        output = subprocess.check_output(self._command(query)).decode('utf-8')

        if self._compare_results(expected, output):
            print('.', end='', flush=True)
        else:
            print('\nERROR')
            print(query)
            print('\nExpected:')
            print(expected)
            print('\nActual:')
            print(output)
            sys.exit(1)


def load_test(path):
    with open(path) as f:
        lines = f.read().split('\n')
        db_file = lines[0]
        tests = []

        is_query = False
        current_query = []
        current_result_set = []
        for line in lines[1:]:
            if line == '':
                continue

            if line == '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>':
                if current_query:
                    tests.append(('\n'.join(current_query), '\n'.join(current_result_set)))
                    current_query = []
                    current_result_set = []
                is_query = True
                continue

            if line == '<<<<<<<<<<<<<<<<<<<<<<<<<<<<<':
                is_query = False
                continue

            if is_query:
                current_query.append(line)
            else:
                current_result_set.append(line)

        if current_query:
            tests.append(('\n'.join(current_query), '\n'.join(current_result_set)))
        return TestFile(path[:-4], db_file, tests)


def load_tests(path):
    return [load_test(os.path.join(path, f)) for f in os.listdir(path)]


if __name__ == '__main__':
    subprocess.check_output(['cargo', 'build'])

    for test in load_tests('tests'):
        test.run()
