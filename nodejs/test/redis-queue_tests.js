'use strict';
// Disable the 'is defined but never used' rule which will show up on stubs.
/* jshint -W098 */

var expect = require('expect.js');
var sinon = require('sinon');
var queue = require('../lib/redis-queue.js');

// Custom matcher that verifies whether two arrays contain the same values
var argsEquals = function(expected_args) {
  return function(array) {
    if (array.length !== expected_args.length) {
      return false;
    }

    for (var i = 0; i < array.length; i++) {
      if (array[i] instanceof Date) { // Handle Date elements
        if (array[i].getTime() !== expected_args[i].getTime()) {
          return false;
        }
      } else {
        if (array[i] !== expected_args[i]) {
          console.log(array[i] + ' !== ' + expected_args[i]);
          return false;
        }
      }
    }

    return true;
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
    it('supplies the correct parameters to evalsha', function() {
      // Arrange
      var queue_name = 'my_queue',
          message = { foo: 'bar' },
          spy = sinon.spy(client_spy, 'evalsha'),
          stub = sinon.stub(client_spy, 'time', function(callback) {
            callback(undefined, [1389535019, 616092]);
          }),
          expected_args = [],
          matcher,
          message_key = '123:abc';

      expected_args.push('');
      expected_args.push(4);
      expected_args.push('message:id');
      expected_args.push('message:received');
      expected_args.push(message_key);
      expected_args.push(queue_name);
      expected_args.push(JSON.stringify(message));
      expected_args.push(new Date(1970, 0, 1, 0, 0, 0, 1389535019616));

      matcher = sinon.match(argsEquals(expected_args));

      // Act
      queue.submit(queue_name, message_key, message);

      // Assert
      expect(spy.calledWith(matcher)).to.be.ok();
    });
  });

  describe('#receive', function() {
  });

  describe('#finish', function() {

  });
});
