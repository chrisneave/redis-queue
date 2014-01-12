
module.exports.redisTimeToJSDate = function(time) {
  var ms_since_epoc = Math.round(time[0] * 1000 + time[1] / 1000);
  return new Date(ms_since_epoc);
}
