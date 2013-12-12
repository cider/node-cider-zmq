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

var EventEmitter = require('events').EventEmitter
  , Q            = require('q')
  , zmq          = require('zmq');

var codec = require('../codec');

//------------------------------------------------------------------------------
// class RPCService
//------------------------------------------------------------------------------

var RPCService = Object.create(EventEmitter.prototype);

/*
 * Constants
 */

const HEADER        = 'CDR#RPC@01'
    , HELLO         = 0x00
    , OHAI          = 0x01
    , KTHXBYE       = 0x02
    , REGISTER      = 0x03
    , UNREGISTER    = 0x04
    , REQUEST       = 0x05
    , INTERRUPT     = 0x06
    , PROGRESS      = 0x07
    , STREAM_FRAME  = 0x08
    , REPLY         = 0x09;

const FRAME_EMPTY         = new Buffer(0)
    , FRAME_HEADER        = new Buffer(HEADER)
    , FRAME_HELLO         = new Buffer([MSGTYPE_HELLO])
    , FRAME_OHAI          = new Buffer([MSGTYPE_OHAI])
    , FRAME_KTHXBYE       = new Buffer([MSGTYPE_KTHXBYE])
    , FRAME_REGISTER      = new Buffer([MSGTYPE_REGISTER])
    , FRAME_UNREGISTER    = new Buffer([MSGTYPE_UNREGISTER])
    , FRAME_REQUEST       = new Buffer([MSGTYPE_REQUEST])
    , FRAME_INTERRUPT     = new Buffer([MSGTYPE_INTERRUPT])
    , FRAME_PROGRESS      = new Buffer([MSGTYPE_PROGRESS])
    , FRAME_STREAM_FRAME  = new Buffer([MSGTYPE_STREAM_FRAME])
    , FRAME_REPLY         = new Buffer([MSGTYPE_REPLY]);

/*
 * RPCService constructor function
 */

function createRPCService(logger, opts) {
  var srv = Object.create(RPCService);
  opts = opts || {};

  var identity = opts.identity    || process.env['CIDER_IDENTITY']
    , endpoint = opts.rpcEndpoint || process.env['CIDER_RPC_ENDPOINT']
    , sndhwm   = opts.rpcSndhwm   || process.env['CIDER_RPC_SNDHWM']
    , rcvhwm   = opts.rpcRcvhwm   || process.env['CIDER_RPC_RCVHWM'];

  if (identity === undefined) return Q.reject(new Error('Cider identity not set'));
  if (endpoint === undefined) return Q.reject( new Error('Cider RPC endpoint not set'));

  var sock;
  try {
    sock = zmq.socket('dealer');
    sock.setsockopt(zmq.ZMQ_IDENTITY, new Buffer(identity));
    if (sndhwm !== undefined) sock.setsockopt(zmq.ZMQ_SNDHWM, parseInt(sndhwm));
    if (rcvhwm !== undefined) sock.setsockopt(zmq.ZMQ_RCVHWM, parseInt(rcvhwm));
  } catch (err) {
    if (sock !== undefined) sock.close();
    return Q.reject(err);
  }

  sock.on('message', srv._handleMessage);
  sock.on('error',   srv._handleError);

  srv._logger = logger;
  srv._sock = sock;

  srv._promise = Q.defer();
  srv._sendHello();
  return srv._promise;
};

// Public API ------------------------------------------------------------------

/*
 * RPCService.close
 */

RPCService.close = function close() {
  this._sendKthxbye();

  var p = Q.defer();
  setTimeout(function() {
    this._sock.close();
    p.resolve();
  }, 5000);

  return p;
};

// Sending messages ------------------------------------------------------------

RPCService._sendHello = function _sendHello() {
  this._rpcSock.send([
      FRAME_HEADER,
      FRAME_HELLO
  ]);
};

RPCService._sendRequest = function _sendRequest() {

};

RPCService._sendInterrupt = function _sendInterrupt() {

};

RPCService._sendStreamFrame = function _sendStreamFrame() {

};

RPCService._sendReply = function _sendReply() {

};

RPCService._sendKthxbye = function _sendHello() {
  this._rpcSock.send([
      FRAME_HEADER,
      FRAME_KTHXBYE
  ]);
};

// Receiving messages ----------------------------------------------------------

/*
 * RPCService._handleMessage
 */

RPCService._handleMessage = function _handleMessage(msg) {
  var msg = arguments;

  if (msg.length < 2) {
    this._logger.warning('Invalid RPC message received: Message too short');
    return;
  }

  if (msg[0].toString() !== HEADER) {
    this._logger.warning('Invalid RPC message received: Header does not match');
    return;
  }

  if (msg[1].length !== 1) {
    this._logger.warning('Invalid RPC message received: Invalid message type');
    return;
  }

  switch (msg[1][0]) {
    case OHAI:
      if (msg.length % 2 !== 0) {
        this._logger.warning('Invalid OHAI message received');
        return;
      }

      var opts = {};
      for (var i = 3; i < msg.length; i += 2) {
        opts[msg[i]] = msg[i+1];
      }

      this._handleOhai(opts);
      break;
    case REQUEST:

      break;
    case INTERRUPT:

      break;
    case PROGRESS:

      break;
    case STREAM_FRAME:

      break;
    default:
      this._logger.warning('Unknown message type received, dropping ...');
  }
};

/*
 * RPCService._handleOhai
 */

RPCService._handleOhai = function _handleOhai(opts, next) {
  var encoding = 'json';
  if (opts['Encoding'] !== undefined) {
    encoding = opts['Encoding'].toString();
  }

  try {
    this._codec = require('./codec').codec(encoding);
  } catch (err) {
    this._close();
    this._next(err);
    return;
  }

  next(null);
};

//------------------------------------------------------------------------------
// Exports
//------------------------------------------------------------------------------

module.exports.createService = createRPCService;
