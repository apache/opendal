<?php

it('basic IO works', function () {
    $op = new OpenDAL(Schema::Memory, []);

    $op->write('test.txt', 'Hello World!');
    expect($op->read('test.txt'))->toBe('Hello World!');
});
