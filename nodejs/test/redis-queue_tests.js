'use strict';
// Disable the 'is defined but never used' rule which will show up on stubs.
/* jshint -W098 */

var expect = require('expect.js');
var sinon = require('sinon');
var queue = require('../lib/redis-queue.js');

describe('redis-queue', function() {
  var client_spy;

  beforeEach(function() {
    client_spy = {
      time: function() {},
      evalsha: function() {}
    }
    queue.init(client_spy);
  });

  describe('#submit', function() {
    it('calls evalsha once', function() {
      // Arrange
      var queue_name = 'my_queue',
          message = { foo: 'bar' },
          spy = sinon.spy(client_spy, 'evalsha'),
          stub = sinon.stub(client_spy, 'time', function(callback) {
            callback(undefined, [1389535019, 616092]);
          }),
          message_key = '123:abc';

      // Act
      queue.submit(queue_name, message_key, message);

      // Assert
      expect(spy.calledOnce).to.be.ok();
    });

    it('passes the name of the destination queue to evalsha', function() {
      // Arrange
      var queue_name = 'my_queue',
          message = { foo: 'bar' },
          spy = sinon.spy(client_spy, 'evalsha'),
          stub = sinon.stub(client_spy, 'time', function(callback) {
            callback(undefined, [1389535019, 616092]);
          }),
          message_key = '123:abc';

      // Act
      queue.submit(queue_name, message_key, message);

      // Assert
      expect(spy.args[0].indexOf(queue_name) >= 0).to.be.ok();
    });

    it('passes the name unique message key to evalsha', function() {
      // Arrange
      var queue_name = 'my_queue',
          message = { foo: 'bar' },
          spy = sinon.spy(client_spy, 'evalsha'),
          stub = sinon.stub(client_spy, 'time', function(callback) {
            callback(undefined, [1389535019, 616092]);
          }),
          message_key = '123:abc';

      // Act
      queue.submit(queue_name, message_key, message);

      // Assert
      expect(spy.args[0].indexOf(message_key) >= 0).to.be.ok();
    });

    it('passes a JSON stringified message to evalsha as an argument', function() {
      // Arrange
      var queue_name = 'my_queue',
          message = { foo: 'bar' },
          spy = sinon.spy(client_spy, 'evalsha'),
          stub = sinon.stub(client_spy, 'time', function(callback) {
            callback(undefined, [1389535019, 616092]);
          }),
          message_key = '123:abc';

      // Act
      queue.submit(queue_name, message_key, message);

      // Assert
      expect(spy.args[0].indexOf(JSON.stringify(message)) >= 0).to.be.ok();
    });

    it('invokes the callback with an undefined error after a successful submission', function(done) {
      // Arrange
      var queue_name = 'my_queue',
          message = { foo: 'bar' },
          evalsha_stub = sinon.stub(client_spy, 'evalsha'),
          stub = sinon.stub(client_spy, 'time', function(callback) {
            callback(undefined, [1389535019, 616092]);
          }),
          message_key = '123:abc';

      evalsha_stub.yields();

      // Act
      queue.submit(queue_name, message_key, message, function(err, result) {
        // Assert
        expect(err).not.to.be.ok();
        done();
      });
    });

    it('invokes the callback with the new message ID after a successful submission', function(done) {
      // Arrange
      var queue_name = 'my_queue',
          message = { foo: 'bar' },
          evalsha_stub = sinon.stub(client_spy, 'evalsha'),
          stub = sinon.stub(client_spy, 'time', function(callback) {
            callback(undefined, [1389535019, 616092]);
          }),
          message_key = '123:abc';

      evalsha_stub.yields(undefined, [1337]);

      // Act
      queue.submit(queue_name, message_key, message, function(err, result) {
        // Assert
        expect(result[0]).to.equal(1337);
        done();
      });
    });
  });

  describe('#receive', function() {
  });

  describe('#finish', function() {

  });
});
