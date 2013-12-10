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

var zmq = require('zmq');

//------------------------------------------------------------------------------
// class RPCService
//------------------------------------------------------------------------------

var RPCService = {};

/*
 * Constants
 */

const HEADER                = 'CDR#RPC@01'
    , MSGTYPE_HELLO         = 0x00
    , MSGTYPE_OHAI          = 0x01
    , MSGTYPE_KTHXBYE       = 0x02
    , MSGTYPE_REGISTER      = 0x03
    , MSGTYPE_UNREGISTER    = 0x04
    , MSGTYPE_REQUEST       = 0x05
    , MSGTYPE_INTERRUPT     = 0x06
    , MSGTYPE_PROGRESS      = 0x07
    , MSGTYPE_STREAM_FRAME  = 0x08
    , MSGTYPE_REPLY         = 0x09;

const FRAME_EMPTY                 = new Buffer(0)
    , FRAME_HEADER                = new Buffer(HEADER)
    , FRAME_MSGTYPE_HELLO         = new Buffer([MSGTYPE_HELLO])
    , FRAME_MSGTYPE_OHAI          = new Buffer([MSGTYPE_OHAI])
    , FRAME_MSGTYPE_KTHXBYE       = new Buffer([MSGTYPE_KTHXBYE])
    , FRAME_MSGTYPE_REGISTER      = new Buffer([MSGTYPE_REGISTER])
    , FRAME_MSGTYPE_UNREGISTER    = new Buffer([MSGTYPE_UNREGISTER])
    , FRAME_MSGTYPE_REQUEST       = new Buffer([MSGTYPE_REQUEST])
    , FRAME_MSGTYPE_INTERRUPT     = new Buffer([MSGTYPE_INTERRUPT])
    , FRAME_MSGTYPE_PROGRESS      = new Buffer([MSGTYPE_PROGRESS])
    , FRAME_MSGTYPE_STREAM_FRAME  = new Buffer([MSGTYPE_STREAM_FRAME])
    , FRAME_MSGTYPE_REPLY         = new Buffer([MSGTYPE_REPLY]);

/*
 * RPCService constructor function
 */

function createRPCService(logger, opts) {
  var socket = Object.create(RPCService);

  var identity = opts.identity    || process.env['CIDER_IDENTITY']
    , endpoint = opts.rpcEndpoint || process.env['CIDER_RPC_ENDPOINT']
    , sndhwm   = opts.rpcSndhwm   || process.env['CIDER_RPC_SNDHWM']
    , rcvhwm   = opts.rpcRcvhwm   || process.env['CIDER_RPC_RCVHWM'];

  if (identity === undefined) throw new Error('Cider identity not set');
  if (endpoint === undefined) throw new Error('Cider RPC endpoint not set');

  var sock = zmq.socket('dealer');
  sock.setsockopt(zmq.ZMQ_IDENTITY, new Buffer(identity));
  if (sndhwm !== undefined) sock.setsockopt(zmq.ZMQ_SNDHWM, parseInt(sndhwm));
  if (rcvhwm !== undefined) sock.setsockopt(zmq.ZMQ_RCVHWM, parseInt(rcvhwm));

  sock.on('message', socket._handleMessage);
  sock.on('error',   socket._handleError);

  socket._logger = logger;
  socket._sock = sock;
  return socket;
};

/*
 * RPCService.connect
 */

RPCService.connect = function connect(next) {
  this._onConnected = next;
  this._sendHello();
};

/*
 * RPCService._sendHello
 */

RPCService._sendHello = function _sendHello() {
  this._logger.debug('Sending HELLO');
  this._rpcSock.send([
      FRAME_EMPTY,
      FRAME_HEADER,
      FRAME_MSGTYPE_HELLO
  ]);
};

/*
 * RPCService._sendKthxbye
 */

/*
 * RPCService._handleMessage
 */

RPCService._handleMessage = function _handleMessage(msg) {
  var msg = arguments;

  if (msg.length < 3) return;
  if (msg[1].toString() !== HEADER) return;
  if (msg[2].length !== 1) return;
  
  switch (msg[2][0]) {
    case MSGTYPE_OHAI:
      var next = this._onConnected;
      this._onConnected = undefined;

      if (next === undefined) {
        this._logger.warning('Duplicate OHAI message received');
        return;
      }

      if (msg.length % 2 !== 1) {
        next(new Error('Invalid OHAI message received'));
        return;
      }

      var opts = {};
      for (i = 3; i < msg.length; i += 2) {
        opts[msg[i]] = msg[i+1];
      }

      this._handleOhai(opts, next);
      break;
    default:
      this._logger.warning('Unknown message type received');
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
