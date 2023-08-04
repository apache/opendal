<?php

describe('basic io', function () {
    $op = new \OpenDAL\Operator('fs', ['root' => '/tmp']);

    it('ensure file not exist', function () use ($op) {
        $op->delete('test.txt');
        expect($op->exist('test.txt'))->toEqual(0);
    });

    it('write/read file', function () use ($op) {
        $op->write('test.txt', 'hello world');
        expect($op->exist('test.txt'))->toEqual(1)
            ->and($op->read('test.txt'))->toEqual('hello world');
    });

    it('write/read file overwrite', function () use ($op) {
        $op->write('test.txt', 'new content');
        expect($op->exist('test.txt'))->toEqual(1)
            ->and($op->read('test.txt'))->toEqual('new content');
    });

    it('file metadata', function () use ($op) {
        $meta = $op->stat('test.txt');
        expect($meta)->toBeInstanceOf(\OpenDAL\Metadata::class)
            ->and($meta->content_length)->toEqual(11)
            ->and($meta->mode)->toBeInstanceOf(\OpenDAL\EntryMode::class)
            ->and($meta->mode->is_file)->toEqual(1)
            ->and($meta->mode->is_dir)->toEqual(0);
    });

    it('delete file', function () use ($op) {
        $op->delete('test.txt');
        expect($op->exist('test.txt'))->toEqual(0);
    });

    it('create dir', function () use ($op) {
        $op->createDir('test/');
        expect(is_dir('/tmp/test'))->toBeTrue();
    });
});
