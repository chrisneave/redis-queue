'use strict';
// Disable the 'is defined but never used' rule which will show up on stubs.
/* jshint -W098 */

var expect = require('expect.js');
var sinon = require('sinon');
var queue = require('../lib/redis-queue.js');

var createArrayContainsMatcher = function(expected_value) {
  return function(array) {
    for (var i = 0; i < array.length; i++) {
      if (new Date(array[i]).getTime() === expected_value.getTime()) { return true; }
    }

    return false;
  }
};

describe('redis-queue', function() {
  var client_spy = {
    time: function() {},
    evalsha: function() {}
  }

  beforeEach(function() {
    queue.init(client_spy);
  });

  describe('#submit', function() {
    it('uses the current Redis server time for the requested_at field', function() {
      // Arrange
      var queue_name = 'my_queue',
          message = { foo: 'bar' },
          expected_date = new Date(1970, 0, 1, 0, 0, 0, 1389535019616),
          spy = sinon.spy(client_spy, 'evalsha'),
          stub = sinon.stub(client_spy, 'time', function(callback) {
            callback(undefined, [1389535019, 616092]);
          });

      // Act
      queue.submit(queue_name, message);

      // Assert
      // *********** Mock the call to evalsha() and expect the value returned by
      // the TIME mock to be passed in as parameter.
      expect(spy.calledWith(sinon.match(createArrayContainsMatcher(expected_date)))).to.be.ok();
    });

    it('posts the message to the queue');
    it('allocates an incremental ID for the message');
  });

  describe('#receive', function() {
  });

  describe('#finish', function() {

  });
});
