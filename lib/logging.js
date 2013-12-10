/*
  Copyright (c) 2013 Ondřej Kupka

  Permission is hereby granted, free of charge, to any person obtaining a copy
  of this software and associated documentation files (the "Software"),
  to deal in the Software without restriction, including without limitation
  the rights to use, copy, modify, merge, publish, distribute, sublicense,
  and/or sell copies of the Software, and to permit persons to whom
  the Software is furnished to do so, subject to the following conditions:

  The above copyright notice and this permission notice shall be included
  in all copies or substantial portions of the Software.

  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
  THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
  FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
  IN THE SOFTWARE.
*/

var zmq  = require('zmq')
  , util = require('util');

//------------------------------------------------------------------------------
// class Logger
//------------------------------------------------------------------------------

var Logger = {};

/*
 * Constants
 */

const HEADER   = 'CDR#LOG@01'
    , TRACE    = 0x00
    , DEBUG    = 0x01
    , INFO     = 0x02
    , WARNING  = 0x03
    , ERROR    = 0x04
    , CRITICAL = 0x05
    , OFF      = 0x06;

const HEADER_FRAME = new Buffer(HEADER)
    , LEVEL_FRAMES = [
          new Buffer([TRACE])
        , new Buffer([DEBUG])
        , new Buffer([INFO])
        , new Buffer([WARNING])
        , new Buffer([ERROR])
        , new Buffer([CRITICAL])
      ]
    , LEVEL_NAMES = [
        , 'TRACE'
        , 'DEBUG'
        , 'INFO'
        , 'WARNING'
        , 'ERROR'
        , 'CRITICAL'
      ];

/*
 * Logger constructor function
 */

function createLogger(opts) {
  var logger = Object.create(Logger);

  var endpoint = opts.endpoint || process.env['CIDER_LOGGING_ENDPOINT']
    , sndhwm   = opts.sndhwm   || process.env['CIDER_LOGGING_SNDHWM']
    , stderr   = opts.stderr   || process.env['CIDER_LOGGING_STDERR']
    , level    = opts.level    || process.env['CIDER_LOGGING_LEVEL'];

  if (endpoint !== undefined && stderr === undefined) {
    var sock = zmq.socket('dealer');
    if (sndhwm !== undefined) sock.setsockopt(zmq.ZMQ_SNDHWM, parseInt(sndhwm));
    sock.connect(endpoint);
    logger._sock = socket;
  }
  else throw new Error('Logging endpoint not defined');

  logger._stderr = stderr;
  logger._level = level || WARNING;
  return logger;
};

/*
 * Logger.setLevel
 */

Logger.setLevel = function setLevel(level) {
  this._level = level;
};

/*
 * Logger.trace
 */

Logger.trace = function trace(msg) {
  this._send(TRACE, msg);
};

/*
 * Logger.debug
 */

Logger.debug = function debug(msg) {
  this._send(DEBUG, msg);
};

/*
 * Logger.info
 */

Logger.info = function info(msg) {
  this._send(INFO, msg);
};

/*
 * Logger.warning
 */

Logger.warning = function warning(msg) {
  this._send(WARNING, msg);
};

/*
 * Logger.error
 */

Logger.error = function error(msg) {
  this._send(ERROR, msg);
};

/*
 * Logger.critical
 */

Logger.critical = function critical(msg) {
  this._send(CRITICAL, msg);
};

/*
 * Logger._send
 */

Logger._send = function _send(level, msg) {
  if (this._level > level) return;

  if (this._stderr !== undefined) {
    process.stderr.write(util.format('[%s] %s\n', LEVEL_NAMES[level], msg));
    if (this._stderr === 'only') return;
  }

  this._sock.send([
      HEADER_FRAME,
      LEVEL_FRAMES[level],
      msg,
  ]);
};

/*
 * Logger.close
 */

Logger.close = function close() {
  if (this._sock !== undefined) this._sock.close();
};

//------------------------------------------------------------------------------
// Exports
//------------------------------------------------------------------------------

module.exports.createService = createLogger;

module.exports.TRACE    = TRACE;
module.exports.DEBUG    = DEBUG;
module.exports.INFO     = INFO;
module.exports.WARNING  = WARNING;
module.exports.ERROR    = ERROR;
module.exports.CRITICAL = CRITICAL;
module.exports.OFF      = OFF;
