#!/bin/bash
set -e
cd "$(dirname "${BASH_SOURCE[0]}")"
cd ..

echo "Building sql-scheduler from source..."
rm -rf dist
python3 -m build >>/dev/null

cd tests

rm -rf .venv
python3 -m venv .venv
source .venv/bin/activate
echo "Installing sql-scheduler in test virtual environment..."
pip3 install "../" >>/dev/null

EXIT_CODE_SUM=0
for test_file in $(ls *.test.py); do
    echo "RUNNING TEST $test_file"
    python3 $test_file || EXIT_CODE_SUM=$(($EXIT_CODE_SUM + $?))
done

if [ $EXIT_CODE_SUM -ne 0 ]; then
    echo "THERE WERE ISSUES WITH TESTS."
fi

deactivate
rm -rf .venv

exit $EXIT_CODE_SUM
