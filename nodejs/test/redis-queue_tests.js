'use strict';
// Disable the 'is defined but never used' rule which will show up on stubs.
/* jshint -W098 */

var expect = require('expect.js');
var sinon = require('sinon');
var queue = require('../lib/redis-queue.js');

describe('redis-queue', function() {
  var client_spy = {
    time: function() {}
  }

  beforeEach(function() {
    queue.init(client_spy);
  });

  describe('#submit', function() {
    it('uses the current Redis server time for the requested_at field', function() {
      // Arrange
      var queue_name = 'my_queue',
          message = { foo: 'bar' },
          spy = sinon.spy(client_spy, 'time');

      // Act
      queue.submit(queue_name, message);

      // Assert
      // *********** Mock the call to evalsha() and expect the value returned by
      // the TIME mock to be passed in as parameter.
      expect(spy.calledOnce).to.be.ok();
    });

    it('posts the message to the queue');
    it('allocates an incremental ID for the message');
  });

  describe('#receive', function() {
  });

  describe('#finish', function() {

  });
});
