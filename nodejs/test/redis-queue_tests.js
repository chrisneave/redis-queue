'use strict';

var expect = require('chai').expect;
var redis_queue = require('../lib/redis-queue.js');

describe('redis-queue', function() {
  it('can new a Queue', function() {
    var queue = new redis_queue.Queue();
    expect(queue).to.be.a('object');
  });
});
