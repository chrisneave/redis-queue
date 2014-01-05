'use strict';
// Disable the 'is defined but never used' rule which will show up on stubs.
/* jshint -W098 */

var expect = require('expect.js');
var sinon = require('sinon');
var queue = require('../lib/redis-queue.js');

describe('redis-queue', function() {
  var _client;

  beforeEach(function() {
    _client = {
      lpush: function() {},
      rpop: function() {}
    };
  });

  describe('#create', function() {
    it('can new a Queue', function() {
      var new_q = queue.create('new queue');
      expect(new_q).to.be.a('object');
    });

    it('must be created with a name', function() {
      var fn = function() {
        queue.create();
      };
      expect(fn).to.throwException('A queue name must be supplied');
    });
  });

  it('has a name', function() {
      var new_q = queue.create('new queue');
      expect(new_q.name).to.equal('new queue');
  });

  describe('#push', function() {
    it('adds a message to the end of the queue', function() {
      // Arrange
      var my_queue = 'new queue',
          my_message = 'my_message',
          spy = sinon.spy(_client, 'lpush'),
          new_q = queue.create(my_queue);

      spy.withArgs(my_queue, my_message);

      // Act
      new_q.push(_client, my_message);

      // Assert
      expect(spy.calledWith(my_queue, my_message)).to.be.ok();
    });
  });

  describe('#pop', function() {
    it ('removes a message from the end of the queue', function(done) {
      // Arrange
      var my_queue = 'my_queue',
          my_message = 'my_message',
          new_q = queue.create(my_queue),
          err,
          stub = sinon.stub(_client, 'rpop', function(queue, callback) {
            callback(err, [my_queue, my_message]);
          });

      // Act
      new_q.pop(_client, function() {
        // Assert
        expect(stub.calledWith(my_queue)).to.be.ok();
        done();
      });
    });

    it('returns the error', function(done) {
      // Arrange
      var my_queue = 'my_queue',
          my_message = 'my_message',
          new_q = queue.create(my_queue),
          err = 'an error',
          stub = sinon.stub(_client, 'rpop', function(queue, callback) {
            callback(err, [my_queue, my_message]);
          });

      // Act
      new_q.pop(_client, function(error) {
        // Assert
        expect(error).to.equal(err);
        done();
      });
    });

    it('returns the message from the queue', function(done) {
      // Arrange
      var my_queue = 'my_queue',
          my_message = 'my_message',
          new_q = queue.create(my_queue),
          err,
          stub = sinon.stub(_client, 'rpop', function(queue, callback) {
            callback(err, [my_queue, my_message]);
          });

      // Act
      new_q.pop(_client, function(error, message) {
        // Assert
        expect(message).to.equal(my_message);
        done();
      });
    });
  });
});
