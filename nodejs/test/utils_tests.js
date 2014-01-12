'use strict';
// Disable the 'is defined but never used' rule which will show up on stubs.
/* jshint -W098 */

var expect = require('expect.js');
var sinon = require('sinon');
var utils = require('../lib/utils.js');

describe('utils', function() {
  describe('#redisTimeToJSDate', function() {
    it('returns a Date representing the Redis TIME structure: [UNIX time in seconds, microseconds]', function() {
      // Arrange
      var time = [1389535019, 616092];
      var expected = new Date(1970, 0, 1, 0, 0, 0, 1389535019616);

      // Act
      var result = utils.redisTimeToJSDate(time);

      // Assert
      expect(result).to.eql(expected);
    });
  });
});
