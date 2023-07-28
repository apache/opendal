<?php

it('opendal-php extension loaded', function () {
    expect(extension_loaded('opendal-php'))->toBeTrue();
});

it('debug function works', function () {
    expect(debug())->toStartWith('Metadata');
});
