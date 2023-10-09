import opendal


def test_sync_write(operator: opendal.Operator):
    filename = 'test_file_name.txt'
    content = b'Hello, world!'
    size = len(content)
    try:
        operator.write(filename, content)
    except Exception as e:
        pytest.fail(f'test_sync_write fail with {e}')
    assert operator.stat(filename)
